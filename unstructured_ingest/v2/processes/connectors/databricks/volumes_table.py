import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator

from pydantic import Field

from unstructured_ingest.utils.data_prep import write_data
from unstructured_ingest.v2.interfaces import FileData, Uploader, UploaderConfig
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.databricks.volumes import DatabricksPathMixin
from unstructured_ingest.v2.processes.connectors.sql.databricks_delta_tables import (
    DatabricksDeltaTablesConnectionConfig,
    DatabricksDeltaTablesUploadStager,
    DatabricksDeltaTablesUploadStagerConfig,
)

CONNECTOR_TYPE = "databricks_volume_delta_tables"


class DatabricksVolumeDeltaTableUploaderConfig(UploaderConfig, DatabricksPathMixin):
    database: str = Field(description="Database name", default="default")
    table_name: str = Field(description="Table name")


@dataclass
class DatabricksVolumeDeltaTableStager(DatabricksDeltaTablesUploadStager):
    def write_output(self, output_path: Path, data: list[dict]) -> None:
        # To avoid new line issues when migrating from volumes into delta tables, omit indenting
        # and always write it as a json file
        write_data(path=output_path.with_suffix(".json"), data=data, indent=None)


@dataclass
class DatabricksVolumeDeltaTableUploader(Uploader):
    connection_config: DatabricksDeltaTablesConnectionConfig
    upload_config: DatabricksVolumeDeltaTableUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        with self.connection_config.get_cursor() as cursor:
            cursor.execute("SHOW CATALOGS")
            catalogs = [r[0] for r in cursor.fetchall()]
            if self.upload_config.catalog not in catalogs:
                raise ValueError(
                    "Catalog {} not found in {}".format(
                        self.upload_config.catalog, ", ".join(catalogs)
                    )
                )
            cursor.execute(f"USE CATALOG '{self.upload_config.catalog}'")
            cursor.execute("SHOW DATABASES")
            databases = [r[0] for r in cursor.fetchall()]
            if self.upload_config.database not in databases:
                raise ValueError(
                    "Database {} not found in {}".format(
                        self.upload_config.database, ", ".join(databases)
                    )
                )
            cursor.execute("SHOW TABLES")
            table_names = [r[1] for r in cursor.fetchall()]
            if self.upload_config.table_name not in table_names:
                raise ValueError(
                    "Table {} not found in {}".format(
                        self.upload_config.table_name, ", ".join(table_names)
                    )
                )

    def get_output_path(self, file_data: FileData, suffix: str = ".json") -> str:
        filename = Path(file_data.source_identifiers.filename)
        adjusted_filename = filename if filename.suffix == suffix else f"{filename}{suffix}"
        return os.path.join(self.upload_config.path, f"{adjusted_filename}")

    @contextmanager
    def get_cursor(self, **connect_kwargs) -> Generator[Any, None, None]:
        with self.connection_config.get_cursor(**connect_kwargs) as cursor:
            logger.debug(f"executing: USE CATALOG: '{self.upload_config.catalog}'")
            cursor.execute(f"USE CATALOG '{self.upload_config.catalog}'")
            logger.debug(f"executing: USE DATABASE: {self.upload_config.database}")
            cursor.execute(f"USE DATABASE {self.upload_config.database}")
            yield cursor

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with self.get_cursor(staging_allowed_local_path=str(path.parent)) as cursor:
            catalog_path = self.get_output_path(file_data=file_data)
            logger.debug(f"uploading {path.as_posix()} to {catalog_path}")
            cursor.execute(f"PUT '{path.as_posix()}' INTO '{catalog_path}' OVERWRITE")
            logger.debug(
                f"migrating content from {catalog_path} to table {self.upload_config.table_name}"
            )
            with path.open() as f:
                data = json.load(f)
                columns = data[0].keys()
            column_str = ", ".join(columns)
            sql_statment = f"INSERT INTO `{self.upload_config.table_name}` ({column_str}) SELECT {column_str} FROM json.`{catalog_path}`"  # noqa: E501
            cursor.execute(sql_statment)


databricks_volumes_delta_tables_destination_entry = DestinationRegistryEntry(
    connection_config=DatabricksDeltaTablesConnectionConfig,
    uploader=DatabricksVolumeDeltaTableUploader,
    uploader_config=DatabricksVolumeDeltaTableUploaderConfig,
    upload_stager=DatabricksVolumeDeltaTableStager,
    upload_stager_config=DatabricksDeltaTablesUploadStagerConfig,
)
