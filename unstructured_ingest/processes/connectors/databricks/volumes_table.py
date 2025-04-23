import json
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.interfaces import (
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
)
from unstructured_ingest.processes.connectors.databricks.volumes import DatabricksPathMixin
from unstructured_ingest.processes.connectors.sql.databricks_delta_tables import (
    DatabricksDeltaTablesConnectionConfig,
    DatabricksDeltaTablesUploadStagerConfig,
)
from unstructured_ingest.utils.constants import RECORD_ID_LABEL
from unstructured_ingest.utils.data_prep import get_enhanced_element_id, get_json_data, write_data

CONNECTOR_TYPE = "databricks_volume_delta_tables"

if TYPE_CHECKING:
    pass


class DatabricksVolumeDeltaTableUploaderConfig(UploaderConfig, DatabricksPathMixin):
    database: str = Field(description="Database name", default="default")
    table_name: Optional[str] = Field(description="Table name", default=None)


class DatabricksVolumeDeltaTableStagerConfig(UploadStagerConfig):
    pass


@dataclass
class DatabricksVolumeDeltaTableStager(UploadStager):
    upload_stager_config: DatabricksVolumeDeltaTableStagerConfig = field(
        default_factory=DatabricksVolumeDeltaTableStagerConfig
    )

    def run(
        self,
        elements_filepath: Path,
        output_dir: Path,
        output_filename: str,
        file_data: FileData,
        **kwargs: Any,
    ) -> Path:
        # To avoid new line issues when migrating from volumes into delta tables, omit indenting
        # and always write it as a json file
        output_dir.mkdir(exist_ok=True, parents=True)
        output_path = output_dir / output_filename
        final_output_path = output_path.with_suffix(".json")
        data = get_json_data(path=elements_filepath)
        for element in data:
            element["id"] = get_enhanced_element_id(element_dict=element, file_data=file_data)
            element[RECORD_ID_LABEL] = file_data.identifier
            element["metadata"] = json.dumps(element.get("metadata", {}))
        write_data(path=final_output_path, data=data, indent=None)
        return final_output_path


@dataclass
class DatabricksVolumeDeltaTableUploader(Uploader):
    connection_config: DatabricksDeltaTablesConnectionConfig
    upload_config: DatabricksVolumeDeltaTableUploaderConfig
    connector_type: str = CONNECTOR_TYPE
    _columns: Optional[dict[str, str]] = None

    def init(self, **kwargs: Any) -> None:
        self.create_destination(**kwargs)

    def create_destination(
        self, destination_name: str = "unstructuredautocreated", **kwargs: Any
    ) -> bool:
        table_name = self.upload_config.table_name or destination_name
        self.upload_config.table_name = table_name
        connectors_dir = Path(__file__).parents[1]
        collection_config_file = connectors_dir / "assets" / "databricks_delta_table_schema.sql"
        with self.get_cursor() as cursor:
            cursor.execute("SHOW TABLES")
            table_names = [r[1] for r in cursor.fetchall()]
            if table_name in table_names:
                return False
            with collection_config_file.open() as schema_file:
                data_lines = schema_file.readlines()
            data_lines[0] = data_lines[0].replace("elements", table_name)
            destination_schema = "".join([line.strip() for line in data_lines])
            logger.info(f"creating table {table_name} for user")
            cursor.execute(destination_schema)
            return True

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

    def get_table_columns(self) -> dict[str, str]:
        if self._columns is None:
            with self.get_cursor() as cursor:
                cursor.execute(f"SELECT * from `{self.upload_config.table_name}` LIMIT 1")
                self._columns = {desc[0]: desc[1] for desc in cursor.description}
        return self._columns

    def can_delete(self) -> bool:
        existing_columns = self.get_table_columns()
        return RECORD_ID_LABEL in existing_columns

    def delete_previous_content(self, file_data: FileData) -> None:
        logger.debug(
            f"deleting any content with metadata "
            f"{RECORD_ID_LABEL}={file_data.identifier} "
            f"from delta table: {self.upload_config.table_name}"
        )
        with self.get_cursor() as cursor:
            cursor.execute(
                f"DELETE FROM `{self.upload_config.table_name}` WHERE {RECORD_ID_LABEL} = '{file_data.identifier}'"  # noqa: E501
            )
            results = cursor.fetchall()
            deleted_rows = results[0][0]
            logger.debug(f"deleted {deleted_rows} rows from table {self.upload_config.table_name}")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        if self.can_delete():
            self.delete_previous_content(file_data=file_data)
        with self.get_cursor(staging_allowed_local_path=path.parent.as_posix()) as cursor:
            catalog_path = self.get_output_path(file_data=file_data)
            logger.debug(f"uploading {path.as_posix()} to {catalog_path}")
            cursor.execute(f"PUT '{path.as_posix()}' INTO '{catalog_path}' OVERWRITE")
            logger.debug(
                f"migrating content from {catalog_path} to table {self.upload_config.table_name}"
            )
            data = get_json_data(path=path)
            columns = data[0].keys()
            select_columns = ["PARSE_JSON(metadata)" if c == "metadata" else c for c in columns]
            column_str = ", ".join(columns)
            select_column_str = ", ".join(select_columns)
            sql_statment = f"INSERT INTO `{self.upload_config.table_name}` ({column_str}) SELECT {select_column_str} FROM json.`{catalog_path}`"  # noqa: E501
            cursor.execute(sql_statment)


databricks_volumes_delta_tables_destination_entry = DestinationRegistryEntry(
    connection_config=DatabricksDeltaTablesConnectionConfig,
    uploader=DatabricksVolumeDeltaTableUploader,
    uploader_config=DatabricksVolumeDeltaTableUploaderConfig,
    upload_stager=DatabricksVolumeDeltaTableStager,
    upload_stager_config=DatabricksDeltaTablesUploadStagerConfig,
)
