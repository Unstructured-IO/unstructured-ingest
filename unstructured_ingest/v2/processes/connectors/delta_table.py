import json
import os
import traceback
from dataclasses import dataclass, field
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import get_data_df
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.table import convert_to_pandas_dataframe
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

CONNECTOR_TYPE = "delta_table"


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
        default=DeltaTableAccessConfig(), validate_default=True
    )
    aws_region: Optional[str] = Field(default=None, description="AWS Region")
    table_uri: str = Field(
        default=None,
        description=(
            "Local path or path to the target folder in the S3 bucket, "
            "formatted as s3://my-bucket/my-folder/"
        ),
    )

    def update_storage_options(self, storage_options: dict) -> None:
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

    def run(
        self,
        elements_filepath: Path,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)

        output_path = Path(output_dir) / Path(f"{output_filename}.parquet")

        df = convert_to_pandas_dataframe(elements_dict=elements_contents)
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

                if self.connection_config.aws_region != response.get("LocationConstraint"):
                    raise ValueError("Wrong AWS Region was provided.")

            except Exception as e:
                logger.error(f"failed to validate connection: {e}", exc_info=True)
                raise DestinationConnectionError(f"failed to validate connection: {e}")

    def upload_dataframe(self, df: pd.DataFrame, file_data: FileData) -> None:
        updated_upload_path = os.path.join(
            self.connection_config.table_uri, file_data.source_identifiers.relative_path
        )
        logger.info(
            f"writing {len(df)} rows to destination table "
            f"at {updated_upload_path}\ndtypes: {df.dtypes}",
        )
        storage_options = {}
        self.connection_config.update_storage_options(storage_options=storage_options)

        writer_kwargs = {
            "table_or_uri": updated_upload_path,
            "data": df,
            "mode": "overwrite",
            "storage_options": storage_options,
        }
        queue = Queue()
        # NOTE: deltalake writer on Linux sometimes can finish but still trigger a SIGABRT and cause
        # ingest to fail, even though all tasks are completed normally. Putting the writer into a
        # process mitigates this issue by ensuring python interpreter waits properly for deltalake's
        # rust backend to finish
        writer = Process(
            target=write_deltalake_with_error_handling,
            kwargs={"queue": queue, **writer_kwargs},
        )
        writer.start()
        writer.join()

        # Check if the queue has any exception message
        if not queue.empty():
            error_message = queue.get()
            logger.error(f"Exception occurred in write_deltalake: {error_message}")
            raise RuntimeError(f"Error in write_deltalake: {error_message}")

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        df = pd.DataFrame(data=data)
        self.upload_dataframe(df=df, file_data=file_data)

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        df = get_data_df(path)
        self.upload_dataframe(df=df, file_data=file_data)


delta_table_destination_entry = DestinationRegistryEntry(
    connection_config=DeltaTableConnectionConfig,
    uploader=DeltaTableUploader,
    uploader_config=DeltaTableUploaderConfig,
    upload_stager=DeltaTableUploadStager,
    upload_stager_config=DeltaTableUploadStagerConfig,
)
