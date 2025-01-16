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
    from vastdb import connect as VastdbConnect
    from vastdb import transaction as VastdbTransaction

CONNECTOR_TYPE = "vastdb"


class VastdbAccessConfig(SQLAccessConfig):
    endpoint: Optional[str] = Field(default=None, description="DB endpoint")
    access_key_id: Optional[str] = Field(default=None, description="access key id")
    access_key_secret: Optional[str] = Field(default=None, description="access key secret")


class VastdbConnectionConfig(SQLConnectionConfig):
    access_config: Secret[VastdbAccessConfig] = Field(
        default=VastdbAccessConfig(), validate_default=True
    )
    database: Optional[str] = Field(
        default=None,
        description="Database name.",
    )
    vastdb_bucket: str
    vastdb_schema: str
    vastdb_table: str
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @contextmanager
    @requires_dependencies(["vastdb"], extras="vastdb")
    def get_connection(self) -> Generator["VastdbConnect", None, None]:
        from vastdb import connect

        access_config = self.access_config.get_secret_value()
        connection = connect(
            endpoint=access_config.endpoint,
            access=access_config.access_key_id,
            secret=access_config.access_key_secret,

        )
        try:
            yield connection
        finally:
            connection.commit()
            connection.close()

    @contextmanager
    def get_cursor(self) -> Generator["VastdbTransaction", None, None]:
        with self.get_connection() as connection:
            cursor = connection.transaction()
            try:
                yield cursor
            finally:
                cursor.close()


class VastdbIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class VastdbIndexer(SQLIndexer):
    connection_config: VastdbConnectionConfig
    index_config: VastdbIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class VastdbDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class VastdbDownloader(SQLDownloader):
    connection_config: VastdbConnectionConfig
    download_config: VastdbDownloaderConfig
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


class VastdbUploadStagerConfig(SQLUploadStagerConfig):
    pass


class VastdbUploadStager(SQLUploadStager):
    upload_stager_config: VastdbUploadStagerConfig


class VastdbUploaderConfig(SQLUploaderConfig):
    pass


@dataclass
class VastdbUploader(SQLUploader):
    upload_config: VastdbUploaderConfig = field(default_factory=VastdbUploaderConfig)
    connection_config: VastdbConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "%s"


postgres_source_entry = SourceRegistryEntry(
    connection_config=VastdbConnectionConfig,
    indexer_config=VastdbIndexerConfig,
    indexer=VastdbIndexer,
    downloader_config=VastdbDownloaderConfig,
    downloader=VastdbDownloader,
)

postgres_destination_entry = DestinationRegistryEntry(
    connection_config=VastdbConnectionConfig,
    uploader=VastdbUploader,
    uploader_config=VastdbUploaderConfig,
    upload_stager=VastdbUploadStager,
    upload_stager_config=VastdbUploadStagerConfig,
)
