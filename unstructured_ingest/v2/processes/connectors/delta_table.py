import json
import os
from dataclasses import dataclass, field
from multiprocessing import Process
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
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

    @requires_dependencies(["s3fs", "fsspec"], extras="s3")
    def precheck(self):
        secrets = self.connection_config.access_config.get_secret_value()
        if (
            self.connection_config.aws_region
            and secrets.aws_access_key_id
            and secrets.aws_secret_access_key
        ):
            from fsspec import get_filesystem_class

            try:
                fs = get_filesystem_class("s3")(
                    key=secrets.aws_access_key_id, secret=secrets.aws_secret_access_key
                )
                fs.write_bytes(path=self.connection_config.table_uri, value=b"")

            except Exception as e:
                logger.error(f"failed to validate connection: {e}", exc_info=True)
                raise DestinationConnectionError(f"failed to validate connection: {e}")

    def process_csv(self, csv_paths: list[Path]) -> pd.DataFrame:
        logger.debug(f"uploading content from {len(csv_paths)} csv files")
        df = pd.concat((pd.read_csv(path) for path in csv_paths), ignore_index=True)
        return df

    def process_json(self, json_paths: list[Path]) -> pd.DataFrame:
        logger.debug(f"uploading content from {len(json_paths)} json files")
        all_records = []
        for p in json_paths:
            with open(p) as json_file:
                all_records.extend(json.load(json_file))

        return pd.DataFrame(data=all_records)

    def process_parquet(self, parquet_paths: list[Path]) -> pd.DataFrame:
        logger.debug(f"uploading content from {len(parquet_paths)} parquet files")
        df = pd.concat((pd.read_parquet(path) for path in parquet_paths), ignore_index=True)
        return df

    def read_dataframe(self, path: Path) -> pd.DataFrame:
        if path.suffix == ".csv":
            return self.process_csv(csv_paths=[path])
        elif path.suffix == ".json":
            return self.process_json(json_paths=[path])
        elif path.suffix == ".parquet":
            return self.process_parquet(parquet_paths=[path])
        else:
            raise ValueError(f"Unsupported file type, must be parquet, json or csv file: {path}")

    @requires_dependencies(["deltalake"], extras="delta-table")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        from deltalake.writer import write_deltalake

        df = self.read_dataframe(path)
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
        # NOTE: deltalake writer on Linux sometimes can finish but still trigger a SIGABRT and cause
        # ingest to fail, even though all tasks are completed normally. Putting the writer into a
        # process mitigates this issue by ensuring python interpreter waits properly for deltalake's
        # rust backend to finish
        writer = Process(
            target=write_deltalake,
            kwargs=writer_kwargs,
        )
        writer.start()
        writer.join()


delta_table_destination_entry = DestinationRegistryEntry(
    connection_config=DeltaTableConnectionConfig,
    uploader=DeltaTableUploader,
    uploader_config=DeltaTableUploaderConfig,
    upload_stager=DeltaTableUploadStager,
    upload_stager_config=DeltaTableUploadStagerConfig,
)
