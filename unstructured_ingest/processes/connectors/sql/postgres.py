from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
    LocationShape,
    SourceRegistryEntry,
)
from unstructured_ingest.processes.connectors.sql.sql import (
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
from unstructured_ingest.utils.dep_check import requires_dependencies

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
        json_schema_extra={"x-runtime-eligible": True},
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
            try:
                connection.commit()
            finally:
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
    table_name: str = Field(json_schema_extra={"x-runtime-eligible": True})


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
                sql.SQL(",").join(
                    sql.Identifier(field)
                    for field in list(dict.fromkeys([id_column] + self.download_config.fields))
                )
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

    @requires_dependencies(["pandas"], extras="postgres")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        super().run(path=path, file_data=file_data, **kwargs)

    @requires_dependencies(["psycopg2"], extras="postgres")
    def classify_write_denial(self, error: Exception, privilege: str) -> Optional[str]:
        """Recognize the two SQLSTATEs that mean the probe's statement was refused.

        psycopg2 puts the server's SQLSTATE on ``pgcode`` and names both of these in
        ``psycopg2.errorcodes``, so neither is spelled out here as a literal. PostgreSQL
        uses ``42501`` for every missing table privilege, so the same code covers the
        INSERT and the DELETE probe; ``privilege`` is what tells them apart in the
        message. Measured on postgres 16: a role with SELECT and INSERT and no DELETE
        passes the INSERT probe and is refused ``42501`` by the DELETE probe.

        ``25006`` matters separately: a hot standby, or a session on a pooler routed to a
        read replica, holds grants that look right in the catalog and refuses every
        write anyway. It is not about one right, so its message names none.

        ``42P01`` (undefined_table) is deliberately not here. PostgreSQL raises it for a
        table that does not exist and for one the credential may not see, and the
        connector cannot tell those apart -- calling a typo'd table name a permissions
        problem is the false refusal this probe exists to avoid.
        """
        from psycopg2 import errorcodes

        pgcode = getattr(error, "pgcode", None)
        if pgcode == errorcodes.INSUFFICIENT_PRIVILEGE:
            return self._write_denied_message(privilege)
        if pgcode == errorcodes.READ_ONLY_SQL_TRANSACTION:
            return (
                f"The destination credentials can connect to the database but the "
                f"connection is read-only, so no record can be written to table "
                f"'{self.upload_config.table_name}'. This is usually a read replica or "
                f"standby endpoint, or a user or database with "
                f"default_transaction_read_only set."
            )
        return None


postgres_source_entry = SourceRegistryEntry(
    connection_config=PostgresConnectionConfig,
    indexer_config=PostgresIndexerConfig,
    indexer=PostgresIndexer,
    downloader_config=PostgresDownloaderConfig,
    downloader=PostgresDownloader,
    location_shape=LocationShape.SQL_TABLE,
    location_identity=("connector_config.database", "indexer_config.table_name"),
    supports_recursion=False,
)

postgres_destination_entry = DestinationRegistryEntry(
    connection_config=PostgresConnectionConfig,
    uploader=PostgresUploader,
    uploader_config=PostgresUploaderConfig,
    upload_stager=PostgresUploadStager,
    upload_stager_config=PostgresUploadStagerConfig,
    location_shape=LocationShape.SQL_TABLE,
    location_identity=("connector_config.database", "uploader_config.table_name"),
    supports_recursion=False,
)
