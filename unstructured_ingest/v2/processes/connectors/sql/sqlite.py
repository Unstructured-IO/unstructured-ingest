import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator

from pydantic import Field, Secret, model_validator

from unstructured_ingest.v2.interfaces import FileData
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.sql.sql import (
    _DATE_COLUMNS,
    SQLAccessConfig,
    SQLConnectionConfig,
    SQLDownloader,
    SQLDownloaderConfig,
    SQLIndexer,
    SQLIndexerConfig,
    SQLUploader,
    SQLUploaderConfig,
    SQLUploadStager,
    SQLUploadStagerConfig,
    parse_date_string,
)

if TYPE_CHECKING:
    from sqlite3 import Connection as SqliteConnection
    from sqlite3 import Cursor as SqliteCursor

CONNECTOR_TYPE = "sqlite"


class SQLiteAccessConfig(SQLAccessConfig):
    pass


class SQLiteConnectionConfig(SQLConnectionConfig):
    access_config: Secret[SQLiteAccessConfig] = Field(
        default=SQLiteAccessConfig(), validate_default=True
    )
    database_path: Path = Field(
        description="Path to the .db file.",
    )

    @model_validator(mode="after")
    def check_database_path(self) -> "SQLiteConnectionConfig":
        if not self.database_path.exists():
            raise ValueError(f"{self.database_path} does not exist")
        if not self.database_path.is_file():
            raise ValueError(f"{self.database_path} is not a valid file")
        return self

    @contextmanager
    def get_connection(self) -> Generator["SqliteConnection", None, None]:
        from sqlite3 import connect

        connection = connect(database=self.database_path)
        try:
            yield connection
        finally:
            connection.commit()
            connection.close()

    @contextmanager
    def get_cursor(self) -> Generator["SqliteCursor", None, None]:
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()


class SQLiteIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class SQLiteIndexer(SQLIndexer):
    connection_config: SQLConnectionConfig
    index_config: SQLIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class SQLiteDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class SQLiteDownloader(SQLDownloader):
    connection_config: SQLConnectionConfig
    download_config: SQLDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    def query_db(self, file_data: FileData) -> tuple[list[tuple], list[str]]:
        table_name = file_data.additional_metadata["table_name"]
        id_column = file_data.additional_metadata["id_column"]
        ids = file_data.additional_metadata["ids"]
        with self.connection_config.get_connection() as sqlite_connection:
            cursor = sqlite_connection.cursor()
            fields = ",".join(self.download_config.fields) if self.download_config.fields else "*"
            query = "SELECT {fields} FROM {table_name} WHERE {id_column} in ({ids})".format(
                fields=fields,
                table_name=table_name,
                id_column=id_column,
                ids=",".join([str(i) for i in ids]),
            )
            logger.debug(f"running query: {query}")
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            return rows, columns


class SQLiteUploadStagerConfig(SQLUploadStagerConfig):
    pass


class SQLiteUploadStager(SQLUploadStager):
    upload_stager_config: SQLiteUploadStagerConfig


class SQLiteUploaderConfig(SQLUploaderConfig):
    pass


@dataclass
class SQLiteUploader(SQLUploader):
    upload_config: SQLiteUploaderConfig = field(default_factory=SQLiteUploaderConfig)
    connection_config: SQLiteConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def prepare_data(
        self, columns: list[str], data: tuple[tuple[Any, ...], ...]
    ) -> list[tuple[Any, ...]]:
        output = []
        for row in data:
            parsed = []
            for column_name, value in zip(columns, row):
                if isinstance(value, (list, dict)):
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


sqlite_source_entry = SourceRegistryEntry(
    connection_config=SQLiteConnectionConfig,
    indexer_config=SQLiteIndexerConfig,
    indexer=SQLIndexer,
    downloader_config=SQLiteDownloaderConfig,
    downloader=SQLiteDownloader,
)

sqlite_destination_entry = DestinationRegistryEntry(
    connection_config=SQLiteConnectionConfig,
    uploader=SQLiteUploader,
    uploader_config=SQLiteUploaderConfig,
    upload_stager=SQLiteUploadStager,
    upload_stager_config=SQLiteUploadStagerConfig,
)
