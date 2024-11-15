import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional

import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
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
    from duckdb import DuckDBPyConnection as DuckDBConnection

CONNECTOR_TYPE = "duckdb"

class DuckDBAccessConfig(AccessConfig):
    pass

class DuckDBConnectionConfig(ConnectionConfig):
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    database: Optional[str] = Field(
        default=None,
        description="Database name. This is the path to the DuckDB .db file. If the file does "
                    "not exist, it will be created at the specified path.",
    )
    schema: Optional[str] = Field(
        default="main",
        description="Schema name. This is the schema within the database where the elements table is located.",
    )
    table: Optional[str] = Field(
        default="elements",
        description="Table name. This is the table name into which the elements data is inserted.",
    )    
    access_config: Secret[DuckDBAccessConfig] = Field(default=DuckDBAccessConfig(), validate_default=True)

    def __post_init__(self):
        if (self.database is None):
            raise ValueError(
                "A DuckDB connection requires a path to a *.db or *.duckdb file "
                "through the `database` argument"
            )        


class DuckDBUploadStagerConfig(UploadStagerConfig):
    pass


_COLUMNS = (
    "id",
    "element_id",
    "text",
    "embeddings",
    "type",
    "system",
    "layout_width",
    "layout_height",
    "points",
    "url",
    "version",
    "date_created",
    "date_modified",
    "date_processed",
    "permissions_data",
    "record_locator",
    "category_depth",
    "parent_id",
    "attached_filename",
    "filetype",
    "last_modified",
    "file_directory",
    "filename",
    "languages",
    "page_number",
    "links",
    "page_name",
    "link_urls",
    "link_texts",
    "sent_from",
    "sent_to",
    "subject",
    "section",
    "header_footer_type",
    "emphasized_text_contents",
    "emphasized_text_tags",
    "text_as_html",
    "regex_metadata",
    "detection_class_prob",
)

# _DATE_COLUMNS = ("date_created", "date_modified", "date_processed", "last_modified")


@dataclass
class DuckDBUploadStager(UploadStager):
    upload_stager_config: DuckDBUploadStagerConfig = field(
        default_factory=lambda: DuckDBUploadStagerConfig()
    )

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents: list[dict] = json.load(elements_file)
        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        output = []
        for data in elements_contents:
            metadata: dict[str, Any] = data.pop("metadata", {})
            data_source = metadata.pop("data_source", {})
            coordinates = metadata.pop("coordinates", {})

            data.update(metadata)
            data.update(data_source)
            data.update(coordinates)

            data["id"] = str(uuid.uuid4())

            # remove extraneous, not supported columns
            data = {k: v for k, v in data.items() if k in _COLUMNS}

            output.append(data)

        df = pd.DataFrame.from_dict(output)

        for column in filter(
            lambda x: x in df.columns,
            ("version", "page_number", "regex_metadata"),
        ):
            df[column] = df[column].apply(str)

        with output_path.open("w") as output_file:
            df.to_json(output_file, orient="records", lines=True)
        return output_path


class DuckDBUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="[Not-used] Number of records per batch")


@dataclass
class DuckDBUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: DuckDBUploaderConfig
    connection_config: DuckDBConnectionConfig

    def precheck(self) -> None:
        try:
            cursor = self.connection().cursor()
            cursor.execute("SELECT 1;")
            cursor.close()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @property
    def connection(self) -> Callable[[], "DuckDBConnection"]:
        return self._make_duckdb_connection

    @requires_dependencies(["duckdb"], extras="duckdb")
    def _make_duckdb_connection(self) -> "DuckDBConnection":
        import duckdb

        return duckdb.connect(self.connection_config.database)    

    def upload_contents(self, path: Path) -> None:
        df_elements = pd.read_json(path, orient="records", lines=True)
        logger.debug(f"uploading {len(df_elements)} entries to {self.connection_config.database} ")

        with self.connection() as conn:
            conn.query(
                f"INSERT INTO {self.connection_config.schema}.{self.connection_config.table} BY NAME SELECT * FROM df_elements"
            )

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        self.upload_contents(path=path)


duckdb_destination_entry = DestinationRegistryEntry(
    connection_config=DuckDBConnectionConfig,
    uploader=DuckDBUploader,
    uploader_config=DuckDBUploaderConfig,
    upload_stager=DuckDBUploadStager,
    upload_stager_config=DuckDBUploadStagerConfig,
)
