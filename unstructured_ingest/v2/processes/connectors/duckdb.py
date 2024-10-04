import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Union

import numpy as np
import pandas as pd
from dateutil import parser
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
ELEMENTS_TABLE_NAME = "elements"
DUCKDB_DB = "duckdb"
MOTHERDUCK_DB = "motherduck"


class MotherDuckAccessConfig(AccessConfig):
    md_token: Optional[str] = Field(default=None, description="MotherDuck token")


class DuckDBConnectionConfig(ConnectionConfig):
    db_type: Literal["duckdb", "motherduck"] = Field(
        default=DUCKDB_DB, description="Type of the database backend"
    )
    database: Optional[str] = Field(
        default=None,
        description="Database name. For duckdb databases, this is the path to the DuckDB .db file.",
    )
    schema: Optional[str] = Field(
        default="main",
        description="Schema name. For duckdb databases, this is the schema within the database where the table is located..",
    )
    access_config: Secret[MotherDuckAccessConfig] = Field(
        default=MotherDuckAccessConfig(), validate_default=True
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    def __post_init__(self):
        if (self.db_type == DUCKDB_DB) and (self.database is None):
            raise ValueError(
                "A DuckDB connection requires a path to a *.db or *.duckdb file "
                "through the `database` argument"
            )

        if self.db_type == MOTHERDUCK_DB:
            if self.database is None:
                raise ValueError(
                    "A MotherDuck connection requires a database (string) to be passed "
                    "through the `database` argument"
                )
            if self.access_config.md_token is None:
                raise ValueError(
                    "A MotherDuck connection requires a md_token (MotherDuck token) to be passed "
                    "using MotherDuckAccessConfig through the `access_config` argument"
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

_DATE_COLUMNS = ("date_created", "date_modified", "date_processed", "last_modified")


def parse_date_string(date_value: Union[str, int]) -> date:
    try:
        timestamp = float(date_value) / 1000 if isinstance(date_value, int) else float(date_value)
        return datetime.fromtimestamp(timestamp)
    except Exception as e:
        logger.debug(f"date {date_value} string not a timestamp: {e}")
    return parser.parse(date_value)


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
    batch_size: int = Field(default=50, description="Number of records per batch")


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
        if self.connection_config.db_type == MOTHERDUCK_DB:
            return self._make_motherduck_connection
        elif self.connection_config.db_type == DUCKDB_DB:
            return self._make_duckdb_connection
        raise ValueError(f"Unsupported database {self.connection_config.db_type} connection.")

    @requires_dependencies(["duckdb"], extras="duckdb")
    def _make_duckdb_connection(self) -> "DuckDBConnection":
        import duckdb

        return duckdb.connect(self.connection_config.database)

    @requires_dependencies(["duckdb"], extras="duckdb")
    def _make_motherduck_connection(self) -> "DuckDBConnection":
        import duckdb

        access_config = self.connection_config.access_config.get_secret_value()
        conn = duckdb.connect(f"md:?motherduck_token={access_config.md_token}")

        conn.sql(f"USE {self.connection_config.database}")

        return conn

    def upload_contents(self, path: Path) -> None:
        df_elements = pd.read_json(path, orient="records", lines=True)
        logger.debug(f"uploading {len(df_elements)} entries to {self.connection_config.database} ")
        # TODO: what happens if this is skipped?
        # df.replace({np.nan: None}, inplace=True)

        # columns = tuple(df_elements.columns)
        # print("\n\n\n\n")
        # for row in tuple(df_elements.itertuples(index=False, name=None)):
        #     for column_name, value in zip(columns, row):
        #         print(f"{column_name} :: {type(value)}")
        #     break
        # print("\n\n\n\n")

        with self.connection() as conn:
            conn.query(f"INSERT INTO {self.connection_config.schema}.{ELEMENTS_TABLE_NAME} BY NAME SELECT * FROM df_elements")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        self.upload_contents(path=path)


duckdb_destination_entry = DestinationRegistryEntry(
    connection_config=DuckDBConnectionConfig,
    uploader=DuckDBUploader,
    uploader_config=DuckDBUploaderConfig,
    upload_stager=DuckDBUploadStager,
    upload_stager_config=DuckDBUploadStagerConfig,
)
