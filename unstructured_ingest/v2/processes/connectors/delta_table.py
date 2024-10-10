import json
from dataclasses import dataclass, field
from multiprocessing import Process
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional

from pydantic import Field, Secret

# from unstructured_ingest.error import DestinationConnectionError
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


if TYPE_CHECKING:
    from deltalake.writer import write_deltalake

CONNECTOR_TYPE = "delta_table"


class DeltaTableAccessConfig(AccessConfig):
    # aws_region: str = Field(
    #     default=None,
    #     description="Region"
    # )
    # aws_access_key_id: str = Field(
    #     default=None,
    #     description="Region"
    # )
    # aws_secret_access_key: str = Field(
    #     default=None,
    #     description="Region"
    # )


class DeltaTableConnectionConfig(ConnectionConfig):
    access_config: Secret[DeltaTableAccessConfig] = Field(
        default=DeltaTableAccessConfig(), validate_default=True
    )
    table_uri: str = Field(
        default=None,
        description="desc",
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
        # file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)

        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        with open(output_path, "w") as output_file:
            json.dump(elements_contents, output_file)
        return output_path


class DeltaTableUploaderConfig(UploaderConfig):
    drop_empty_cols: bool = False
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
        from deltalake.writer import write_deltalake

        from unstructured_ingest.utils.table import convert_to_pandas_dataframe

        with path.open("r") as file:
            elements_dict = json.load(file)

        df = convert_to_pandas_dataframe(
            elements_dict=elements_dict,
            drop_empty_cols=self.upload_config.drop_empty_cols,
        )
        logger.info(
            f"writing {len(df)} rows to destination table "
            f"at {self.connection_config.table_uri}\ndtypes: {df.dtypes}",
        )
        # storage_options = {
        #     "AWS_REGION": self.connection_config.access_config.get_secret_value().aws_region,
        #     "AWS_ACCESS_KEY_ID": self.connection_config.access_config.get_secret_value().aws_access_key_id,
        #     "AWS_SECRET_ACCESS_KEY": self.connection_config.access_config.get_secret_value().aws_secret_access_key,
        # }
        writer_kwargs = {
            "table_or_uri": self.connection_config.table_uri,
            "data": df,
            "mode": self.upload_config.mode,
            "engine": self.upload_config.engine,
            # "storage_options": storage_options,
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
