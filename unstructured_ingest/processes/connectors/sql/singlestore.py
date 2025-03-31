import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.processes.connectors.sql.sql import (
    _DATE_COLUMNS,
    SQLAccessConfig,
    SqlBatchFileData,
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
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from singlestoredb.connection import Connection as SingleStoreConnection
    from singlestoredb.connection import Cursor as SingleStoreCursor

CONNECTOR_TYPE = "singlestore"


class SingleStoreAccessConfig(SQLAccessConfig):
    password: Optional[str] = Field(default=None, description="SingleStore password")


class SingleStoreConnectionConfig(SQLConnectionConfig):
    access_config: Secret[SingleStoreAccessConfig]
    host: Optional[str] = Field(default=None, description="SingleStore host")
    port: Optional[int] = Field(default=None, description="SingleStore port")
    user: Optional[str] = Field(default=None, description="SingleStore user")
    database: Optional[str] = Field(default=None, description="SingleStore database")

    @contextmanager
    @requires_dependencies(["singlestoredb"], extras="singlestore")
    def get_connection(self) -> Generator["SingleStoreConnection", None, None]:
        import singlestoredb as s2

        connection = s2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.access_config.get_secret_value().password,
        )
        try:
            yield connection
        finally:
            connection.commit()
            connection.close()

    @contextmanager
    def get_cursor(self) -> Generator["SingleStoreCursor", None, None]:
        with self.get_connection() as connection, connection.cursor() as cursor:
            try:
                yield cursor
            finally:
                cursor.close()


class SingleStoreIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class SingleStoreIndexer(SQLIndexer):
    connection_config: SingleStoreConnectionConfig
    index_config: SingleStoreIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class SingleStoreDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class SingleStoreDownloader(SQLDownloader):
    connection_config: SingleStoreConnectionConfig
    download_config: SingleStoreDownloaderConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "%s"

    def query_db(self, file_data: SqlBatchFileData) -> tuple[list[tuple], list[str]]:
        table_name = file_data.additional_metadata.table_name
        id_column = file_data.additional_metadata.id_column
        ids = tuple([item.identifier for item in file_data.batch_items])
        with self.connection_config.get_connection() as sqlite_connection:
            cursor = sqlite_connection.cursor()
            fields = ",".join(self.download_config.fields) if self.download_config.fields else "*"
            query = (
                f"SELECT {fields} FROM {table_name} WHERE {id_column} IN {self.values_delimiter}"
            )
            logger.debug(f"running query: {query}\nwith values: {(ids,)}")
            cursor.execute(query, (ids,))
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            return rows, columns


class SingleStoreUploadStagerConfig(SQLUploadStagerConfig):
    pass


class SingleStoreUploadStager(SQLUploadStager):
    upload_stager_config: SingleStoreUploadStagerConfig


class SingleStoreUploaderConfig(SQLUploaderConfig):
    pass


@dataclass
class SingleStoreUploader(SQLUploader):
    upload_config: SingleStoreUploaderConfig = field(default_factory=SingleStoreUploaderConfig)
    connection_config: SingleStoreConnectionConfig
    values_delimiter: str = "%s"
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["pandas"], extras="singlestore")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        super().run(path=path, file_data=file_data, **kwargs)

    @requires_dependencies(["pandas"], extras="singlestore")
    def prepare_data(
        self, columns: list[str], data: tuple[tuple[Any, ...], ...]
    ) -> list[tuple[Any, ...]]:
        import pandas as pd

        output = []
        for row in data:
            parsed = []
            for column_name, value in zip(columns, row):
                if isinstance(value, (list, dict)):
                    value = json.dumps(value)
                if column_name in _DATE_COLUMNS:
                    if value is None or pd.isna(value):
                        parsed.append(None)
                    else:
                        parsed.append(parse_date_string(value))
                else:
                    parsed.append(value)
            output.append(tuple(parsed))
        return output


singlestore_source_entry = SourceRegistryEntry(
    connection_config=SingleStoreConnectionConfig,
    indexer_config=SingleStoreIndexerConfig,
    indexer=SingleStoreIndexer,
    downloader_config=SingleStoreDownloaderConfig,
    downloader=SingleStoreDownloader,
)

singlestore_destination_entry = DestinationRegistryEntry(
    connection_config=SingleStoreConnectionConfig,
    uploader=SingleStoreUploader,
    uploader_config=SingleStoreUploaderConfig,
    upload_stager=SingleStoreUploadStager,
    upload_stager_config=SingleStoreUploadStagerConfig,
)
