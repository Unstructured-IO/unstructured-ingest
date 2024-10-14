import json
from dataclasses import dataclass, field
from multiprocessing import Process
from pathlib import Path
from typing import Any, Literal, Optional

from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
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
    aws_region: Optional[str] = Field(default=None, description="AWS Region")
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS Access Key Id")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS Secret Access Key")


class DeltaTableConnectionConfig(ConnectionConfig):
    access_config: Secret[DeltaTableAccessConfig] = Field(
        default=DeltaTableAccessConfig(), validate_default=True
    )
    table_uri: str = Field(
        default=None,
        description="The path to the target folder in the S3 bucket,"
        "formatted as s3://my-bucket/my-folder/ or local path",
    )


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
        from unstructured_ingest.utils.table import convert_to_pandas_dataframe

        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)

        output_path = Path(output_dir) / Path(f"{output_filename}.json")

        df = convert_to_pandas_dataframe(elements_dict=elements_contents)
        df.to_parquet(output_path)

        return output_path


class DeltaTableUploaderConfig(UploaderConfig):
    mode: Literal["error", "append", "overwrite", "ignore"] = "error"
    schema_mode: Optional[Literal["merge", "overwrite"]] = None
    engine: Literal["pyarrow", "rust"] = "pyarrow"


@dataclass
class DeltaTableUploader(Uploader):
    upload_config: DeltaTableUploaderConfig
    connection_config: DeltaTableConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["deltalake"], extras="delta-table")
    def precheck(self):
        pass

    @requires_dependencies(["deltalake"], extras="delta-table")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        import pandas as pd
        from deltalake.writer import write_deltalake

        df = pd.read_parquet(path)
        logger.info(
            f"writing {len(df)} rows to destination table "
            f"at {self.connection_config.table_uri}\ndtypes: {df.dtypes}",
        )
        storage_options = {}
        secrets = self.connection_config.access_config.get_secret_value()
        if secrets.aws_access_key_id and secrets.aws_secret_access_key:
            storage_options["AWS_REGION"] = secrets.aws_region
            storage_options["AWS_ACCESS_KEY_ID"] = secrets.aws_access_key_id
            storage_options["AWS_SECRET_ACCESS_KEY"] = secrets.aws_secret_access_key
            storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

        writer_kwargs = {
            "table_or_uri": self.connection_config.table_uri,
            "data": df,
            "mode": self.upload_config.mode,
            "engine": self.upload_config.engine,
            "storage_options": storage_options,
        }
        if self.upload_config.schema_mode is not None:
            writer_kwargs["schema_mode"] = self.upload_config.schema_mode
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
