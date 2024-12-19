from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.sql.sql import (
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
)

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PostgresConnection
    from psycopg2.extensions import cursor as PostgresCursor

CONNECTOR_TYPE = "postgres"


class PostgresAccessConfig(SQLAccessConfig):
    password: Optional[str] = Field(default=None, description="DB password")


class PostgresConnectionConfig(SQLConnectionConfig):
    access_config: Secret[PostgresAccessConfig] = Field(
        default=PostgresAccessConfig(), validate_default=True
    )
    database: Optional[str] = Field(
        default=None,
        description="Database name.",
    )
    username: Optional[str] = Field(default=None, description="DB username")
    host: Optional[str] = Field(default=None, description="DB host")
    port: Optional[int] = Field(default=5432, description="DB host connection port")
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @contextmanager
    @requires_dependencies(["psycopg2"], extras="postgres")
    def get_connection(self) -> Generator["PostgresConnection", None, None]:
        from psycopg2 import connect

        access_config = self.access_config.get_secret_value()
        connection = connect(
            user=self.username,
            password=access_config.password,
            dbname=self.database,
            host=self.host,
            port=self.port,
        )
        try:
            yield connection
        finally:
            connection.commit()
            connection.close()

    @contextmanager
    def get_cursor(self) -> Generator["PostgresCursor", None, None]:
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()


class PostgresIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class PostgresIndexer(SQLIndexer):
    connection_config: PostgresConnectionConfig
    index_config: PostgresIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class PostgresDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class PostgresDownloader(SQLDownloader):
    connection_config: PostgresConnectionConfig
    download_config: PostgresDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["psycopg2"], extras="postgres")
    def query_db(self, file_data: SqlBatchFileData) -> tuple[list[tuple], list[str]]:
        from psycopg2 import sql

        table_name = file_data.additional_metadata.table_name
        id_column = file_data.additional_metadata.id_column
        ids = tuple([item.identifier for item in file_data.batch_items])

        with self.connection_config.get_cursor() as cursor:
            fields = (
                sql.SQL(",").join(sql.Identifier(field) for field in self.download_config.fields)
                if self.download_config.fields
                else sql.SQL("*")
            )

            query = sql.SQL("SELECT {fields} FROM {table_name} WHERE {id_column} IN %s").format(
                fields=fields,
                table_name=sql.Identifier(table_name),
                id_column=sql.Identifier(id_column),
            )
            logger.debug(f"running query: {cursor.mogrify(query, (ids,))}")
            cursor.execute(query, (ids,))
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            return rows, columns


class PostgresUploadStagerConfig(SQLUploadStagerConfig):
    pass


class PostgresUploadStager(SQLUploadStager):
    upload_stager_config: PostgresUploadStagerConfig


class PostgresUploaderConfig(SQLUploaderConfig):
    pass


@dataclass
class PostgresUploader(SQLUploader):
    upload_config: PostgresUploaderConfig = field(default_factory=PostgresUploaderConfig)
    connection_config: PostgresConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "%s"


postgres_source_entry = SourceRegistryEntry(
    connection_config=PostgresConnectionConfig,
    indexer_config=PostgresIndexerConfig,
    indexer=PostgresIndexer,
    downloader_config=PostgresDownloaderConfig,
    downloader=PostgresDownloader,
)

postgres_destination_entry = DestinationRegistryEntry(
    connection_config=PostgresConnectionConfig,
    uploader=PostgresUploader,
    uploader_config=PostgresUploaderConfig,
    upload_stager=PostgresUploadStager,
    upload_stager_config=PostgresUploadStagerConfig,
)
