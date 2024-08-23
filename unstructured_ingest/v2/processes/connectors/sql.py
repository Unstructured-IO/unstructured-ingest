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
    from sqlite3 import Connection as SqliteConnection

    from psycopg2.extensions import connection as PostgresConnection

CONNECTOR_TYPE = "sql"
ELEMENTS_TABLE_NAME = "elements"
SQLITE_DB = "sqlite"
POSTGRESQL_DB = "postgresql"


class SQLAccessConfig(AccessConfig):
    username: Optional[str] = Field(default=None, description="DB username")
    password: Optional[str] = Field(default=None, description="DB password")


SecreteSQLAccessConfig = Secret[SQLAccessConfig]


class SQLConnectionConfig(ConnectionConfig):
    db_type: Literal["sqlite", "postgresql"] = Field(
        default=SQLITE_DB, description="Type of the database backend"
    )
    database: Optional[str] = Field(
        default=None,
        description="Database name. For sqlite databases, this is the path to the .db file.",
    )
    host: Optional[str] = Field(default=None, description="DB host")
    port: Optional[int] = Field(default=5432, description="DB host connection port")
    access_config: SecreteSQLAccessConfig = Field(
        default_factory=lambda: SecreteSQLAccessConfig(secret_value=SQLAccessConfig())
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    def __post_init__(self):
        if (self.db_type == SQLITE_DB) and (self.database is None):
            raise ValueError(
                "A sqlite connection requires a path to a *.db file "
                "through the `database` argument"
            )


class SQLUploadStagerConfig(UploadStagerConfig):
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
class SQLUploadStager(UploadStager):
    upload_stager_config: SQLUploadStagerConfig = field(
        default_factory=lambda: SQLUploadStagerConfig()
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
        for column in filter(lambda x: x in df.columns, _DATE_COLUMNS):
            df[column] = df[column].apply(parse_date_string)
        for column in filter(
            lambda x: x in df.columns,
            ("permissions_data", "record_locator", "points", "links"),
        ):
            df[column] = df[column].apply(
                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None
            )
        for column in filter(
            lambda x: x in df.columns,
            ("version", "page_number", "regex_metadata"),
        ):
            df[column] = df[column].apply(str)

        with output_path.open("w") as output_file:
            df.to_json(output_file, orient="records", lines=True)
        return output_path


class SQLUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="Number of records per batch")


@dataclass
class SQLUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: SQLUploaderConfig
    connection_config: SQLConnectionConfig

    def precheck(self) -> None:
        try:
            cursor = self.connection().cursor()
            cursor.execute("SELECT 1;")
            cursor.close()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @property
    def connection(self) -> Callable[[], Union["SqliteConnection", "PostgresConnection"]]:
        if self.connection_config.db_type == POSTGRESQL_DB:
            return self._make_psycopg_connection
        elif self.connection_config.db_type == SQLITE_DB:
            return self._make_sqlite_connection
        raise ValueError(f"Unsupported database {self.connection_config.db_type} connection.")

    def _make_sqlite_connection(self) -> "SqliteConnection":
        from sqlite3 import connect

        return connect(database=self.connection_config.database)

    @requires_dependencies(["psycopg2"], extras="postgres")
    def _make_psycopg_connection(self) -> "PostgresConnection":
        from psycopg2 import connect

        access_config = self.connection_config.access_config.get_secret_value()
        return connect(
            user=access_config.username,
            password=access_config.password,
            dbname=self.connection_config.database,
            host=self.connection_config.host,
            port=self.connection_config.port,
        )

    def prepare_data(
        self, columns: list[str], data: tuple[tuple[Any, ...], ...]
    ) -> list[tuple[Any, ...]]:
        output = []
        for row in data:
            parsed = []
            for column_name, value in zip(columns, row):
                if self.connection_config.db_type == SQLITE_DB and isinstance(value, (list, dict)):
                    value = json.dumps(value)
                if column_name in _DATE_COLUMNS:
                    if value is None:
                        parsed.append(None)
                    else:
                        parsed.append(parse_date_string(value))
                else:
                    parsed.append(value)
            output.append(tuple(parsed))
        return output

    def upload_contents(self, path: Path) -> None:
        df = pd.read_json(path, orient="records", lines=True)
        logger.debug(f"uploading {len(df)} entries to {self.connection_config.database} ")
        df.replace({np.nan: None}, inplace=True)

        columns = tuple(df.columns)
        stmt = f"INSERT INTO {ELEMENTS_TABLE_NAME} ({','.join(columns)}) \
                VALUES({','.join(['?' if self.connection_config.db_type==SQLITE_DB else '%s' for x in columns])})"  # noqa E501

        for rows in pd.read_json(
            path, orient="records", lines=True, chunksize=self.upload_config.batch_size
        ):
            with self.connection() as conn:
                values = self.prepare_data(columns, tuple(rows.itertuples(index=False, name=None)))
                if self.connection_config.db_type == SQLITE_DB:
                    conn.executemany(stmt, values)
                else:
                    with conn.cursor() as cur:
                        cur.executemany(stmt, values)

                conn.commit()

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        self.upload_contents(path=path)


sql_destination_entry = DestinationRegistryEntry(
    connection_config=SQLConnectionConfig,
    uploader=SQLUploader,
    uploader_config=SQLUploaderConfig,
    upload_stager=SQLUploadStager,
    upload_stager_config=SQLUploadStagerConfig,
)
