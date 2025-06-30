import logging
import traceback
from dataclasses import dataclass, field
from multiprocessing import Queue, current_process
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlparse

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.utils.constants import RECORD_ID_LABEL
from unstructured_ingest.utils.data_prep import get_data_df, get_json_data
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.table import convert_to_pandas_dataframe

CONNECTOR_TYPE = "delta_table"

if TYPE_CHECKING:
    from pandas import DataFrame


@requires_dependencies(["deltalake"], extras="delta-table")
def write_deltalake_with_error_handling(queue, **kwargs):
    from deltalake.writer import write_deltalake

    try:
        write_deltalake(**kwargs)
    except Exception:
        queue.put(traceback.format_exc())


class DeltaTableAccessConfig(AccessConfig):
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS Access Key Id")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS Secret Access Key")


class DeltaTableConnectionConfig(ConnectionConfig):
    access_config: Secret[DeltaTableAccessConfig] = Field(
        default=Secret(DeltaTableAccessConfig()), validate_default=True
    )
    aws_region: Optional[str] = Field(default=None, description="AWS Region")
    table_uri: str = Field(
        description=(
            "Local path or path to the target folder in the S3 bucket, "
            "formatted as s3://my-bucket/my-folder/"
        ),
    )

    def update_storage_options(self, storage_options: dict[str, str]) -> None:
        secrets = self.access_config.get_secret_value()
        if self.aws_region and secrets.aws_access_key_id and secrets.aws_secret_access_key:
            storage_options["AWS_REGION"] = self.aws_region
            storage_options["AWS_ACCESS_KEY_ID"] = secrets.aws_access_key_id
            storage_options["AWS_SECRET_ACCESS_KEY"] = secrets.aws_secret_access_key
            # Delta-rs doesn't support concurrent S3 writes without external locks (DynamoDB).
            # This flag allows single-writer uploads to S3 without using locks, according to:
            # https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/
            storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"


class DeltaTableUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class DeltaTableUploadStager(UploadStager):
    upload_stager_config: DeltaTableUploadStagerConfig = field(
        default_factory=lambda: DeltaTableUploadStagerConfig()
    )

    def run(  # type: ignore[override]
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        elements_contents = get_json_data(elements_filepath)
        output_path = Path(output_dir) / Path(f"{output_filename}.parquet")

        df = convert_to_pandas_dataframe(elements_dict=elements_contents)
        # Ensure per-record overwrite/delete semantics: tag each row with the record identifier
        df[RECORD_ID_LABEL] = file_data.identifier
        df = df.dropna(axis=1, how="all")
        df.to_parquet(output_path)

        return output_path


class DeltaTableUploaderConfig(UploaderConfig):
    pass


@dataclass
class DeltaTableUploader(Uploader):
    upload_config: DeltaTableUploaderConfig
    connection_config: DeltaTableConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["boto3"], extras="delta-table")
    def precheck(self):
        secrets = self.connection_config.access_config.get_secret_value()
        if (
            self.connection_config.aws_region
            and secrets.aws_access_key_id
            and secrets.aws_secret_access_key
        ):
            from boto3 import client

            url = urlparse(self.connection_config.table_uri)
            bucket_name = url.netloc
            dir_path = url.path.lstrip("/")

            try:
                s3_client = client(
                    "s3",
                    aws_access_key_id=secrets.aws_access_key_id,
                    aws_secret_access_key=secrets.aws_secret_access_key,
                )
                s3_client.put_object(Bucket=bucket_name, Key=dir_path, Body=b"")

                response = s3_client.get_bucket_location(Bucket=bucket_name)

                bucket_region = _normalize_location_constraint(response.get("LocationConstraint"))

                if self.connection_config.aws_region != bucket_region:
                    raise ValueError(
                        "Wrong AWS region provided: bucket "
                        f"'{bucket_name}' resides in '{bucket_region}', "
                        "but configuration specifies "
                        f"'{self.connection_config.aws_region}'."
                    )

            except Exception as e:
                logger.error(f"failed to validate connection: {e}", exc_info=True)
                raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["tenacity"], extras="delta-table")
    def upload_dataframe(self, df: "DataFrame", file_data: FileData) -> None:
        upload_path = self.connection_config.table_uri
        logger.info(
            f"writing {len(df)} rows to destination table at {upload_path}\ndtypes: {df.dtypes}",
        )
        storage_options: dict[str, str] = {}
        self.connection_config.update_storage_options(storage_options=storage_options)

        # Decide whether the Delta table already exists. If it does, we first delete all rows
        # belonging to the current record and then append the fresh data. Otherwise we will
        # create a brand-new table via an overwrite.

        mode = "overwrite"
        try:
            from deltalake import DeltaTable  # pylint: disable=import-error

            dt = DeltaTable(upload_path, storage_options=storage_options)
            logger.debug(f"Table exists: deleting rows for {file_data.identifier}")
            # Table exists – remove any previous rows for this record_id so that appending is
            # effectively an idempotent overwrite for the record.
            dt.delete(predicate=f"{RECORD_ID_LABEL} = '{file_data.identifier}'")
            mode = "append"
        except Exception:
            # Table does not exist yet (or cannot be opened) – we will create it below with
            # mode="overwrite". All other failures will be captured later by the writer.
            logger.debug("Table does not exist: creating new table")

        writer_kwargs = {
            "table_or_uri": upload_path,
            "data": df,
            "mode": mode,
            "schema_mode": "merge",
            "storage_options": storage_options,
        }

        from tenacity import (
            before_log,
            retry,
            retry_if_exception,
            stop_after_attempt,
            wait_random,
        )

        def _is_commit_conflict(exc: BaseException) -> bool:  # noqa: ANN401
            """Return True if exception looks like a Delta Lake commit conflict.

            Besides the canonical *CommitFailed* / *Metadata changed* errors that
            deltalake surfaces when two writers clash, we occasionally hit
            messages such as *Delta transaction failed, version 0 already
            exists* while multiple processes race to create the very first log
            entry. These situations are equally safe to retry, so detect them
            too.
            """

            return isinstance(exc, RuntimeError) and any(
                marker in str(exc)
                for marker in (
                    "CommitFailed",
                    "Metadata changed",
                    "version 0 already exists",
                    "version already exists",
                    "Delta transaction failed",
                )
            )

        @retry(
            stop=stop_after_attempt(10),
            wait=wait_random(min=0.2, max=1.0),
            before=before_log(logger, logging.DEBUG),
            retry=retry_if_exception(_is_commit_conflict),
            reraise=True,
        )
        def _single_attempt() -> None:
            """One optimistic transaction: delete old rows, then append new ones."""

            # NOTE: deltalake writer on Linux sometimes can finish but still trigger a SIGABRT and
            # cause ingest to fail, even though all tasks are completed normally. Putting the writer
            # into a process mitigates this issue by ensuring python interpreter waits properly for
            # deltalake's rust backend to finish
            # Use a multiprocessing context that relies on 'spawn' to avoid inheriting the
            # parent process' Tokio runtime, which leads to `pyo3_runtime.PanicException`.
            from multiprocessing import get_context

            ctx = get_context("spawn")
            queue: "Queue[str]" = ctx.Queue()

            if current_process().daemon:
                # write_deltalake_with_error_handling will push any traceback to our queue
                write_deltalake_with_error_handling(queue=queue, **writer_kwargs)
            else:
                # On non-daemon processes we still guard against SIGABRT by running in a
                # dedicated subprocess created via the 'spawn' method.
                writer = ctx.Process(
                    target=write_deltalake_with_error_handling,
                    kwargs={"queue": queue, **writer_kwargs},
                )
                writer.start()
                writer.join()

                # First surface any traceback captured inside the subprocess so users see the real
                # root-cause instead of a generic non-zero exit code.
                if not queue.empty():
                    error_message = queue.get()
                    logger.error("Exception occurred in write_deltalake: %s", error_message)
                    raise RuntimeError(f"Error in write_deltalake: {error_message}")

                # If the subprocess terminated abnormally but produced no traceback (e.g., SIGABRT),
                # still raise a helpful error for callers.
                if not current_process().daemon and writer.exitcode != 0:
                    raise RuntimeError(
                        f"write_deltalake subprocess exited with code {writer.exitcode}"
                    )

        _single_attempt()

    @requires_dependencies(["pandas"], extras="delta-table")
    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        import pandas as pd

        df = pd.DataFrame(data=data)
        self.upload_dataframe(df=df, file_data=file_data)

    @requires_dependencies(["pandas"], extras="delta-table")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:  # type: ignore[override]
        df = get_data_df(path)
        self.upload_dataframe(df=df, file_data=file_data)


def _normalize_location_constraint(location: Optional[str]) -> str:
    """Return canonical AWS region name for a LocationConstraint value.

    The S3 GetBucketLocation operation returns `null` (`None`) for buckets in
    the legacy `us-east-1` region and `EU` for very old buckets that were
    created in the historical `EU` region (now `eu-west-1`). For every other
    region the API already returns the correct AWS region string. This helper
    normalises the legacy values so callers can reliably compare regions.

    Args:
        location: The LocationConstraint value returned by the S3 GetBucketLocation operation.

    Returns:
        The canonical AWS region name for the given location constraint.
    """

    if location is None:
        return "us-east-1"
    if location == "EU":
        return "eu-west-1"
    return location


delta_table_destination_entry = DestinationRegistryEntry(
    connection_config=DeltaTableConnectionConfig,
    uploader=DeltaTableUploader,
    uploader_config=DeltaTableUploaderConfig,
    upload_stager=DeltaTableUploadStager,
    upload_stager_config=DeltaTableUploadStagerConfig,
)
