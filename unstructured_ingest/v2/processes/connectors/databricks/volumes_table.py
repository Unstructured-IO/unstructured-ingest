import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, Optional

from pydantic import Field

from unstructured_ingest.v2.interfaces import FileData, Uploader, UploaderConfig
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.databricks.volumes import DatabricksPathMixin
from unstructured_ingest.v2.processes.connectors.sql.databricks_delta_tables import (
    DatabrickDeltaTablesConnectionConfig,
    DatabrickDeltaTablesUploadStager,
    DatabrickDeltaTablesUploadStagerConfig,
)

CONNECTOR_TYPE = "databricks_volume_delta_tables"


class DatabricksVolumeDeltaTableUploaderConfig(UploaderConfig, DatabricksPathMixin):
    database: str = Field(description="Database name", default="default")
    table_name: str = Field(description="Table name")


@dataclass
class DatabricksVolumeDeltaTableStager(DatabrickDeltaTablesUploadStager):
    def write_output(self, output_path: Path, data: list[dict], indent: Optional[int] = 2) -> None:
        # To avoid new line issues when migrating from volumes into delta tables, omit indenting
        # and always write it as a json file
        with output_path.with_suffix(".json").open("w") as f:
            json.dump(data, f)


@dataclass
class DatabricksVolumeDeltaTableUploader(Uploader):
    connection_config: DatabrickDeltaTablesConnectionConfig
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
            cursor.execute(f"USE CATALOG '{self.upload_config.catalog}'")
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
    connection_config=DatabrickDeltaTablesConnectionConfig,
    uploader=DatabricksVolumeDeltaTableUploader,
    uploader_config=DatabricksVolumeDeltaTableUploaderConfig,
    upload_stager=DatabricksVolumeDeltaTableStager,
    upload_stager_config=DatabrickDeltaTablesUploadStagerConfig,
)
