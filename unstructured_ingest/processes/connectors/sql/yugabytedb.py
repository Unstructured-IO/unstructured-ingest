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
    from psycopg2.extensions import connection as YugabyteDbConnection
    from psycopg2.extensions import cursor as YugabyteDbCursor

CONNECTOR_TYPE = "yugabytedb"


class YugabyteDbAccessConfig(SQLAccessConfig):
    password: Optional[str] = Field(default=None, description="DB password")


class YugabyteDbConnectionConfig(SQLConnectionConfig):
    access_config: Secret[YugabyteDbAccessConfig] = Field(
        default=YugabyteDbAccessConfig(), validate_default=True
    )
    database: Optional[str] = Field(
        default=None,
        description="Database name.",
    )
    username: Optional[str] = Field(default=None, description="DB username")
    host: Optional[str] = Field(default=None, description="DB host")
    port: Optional[int] = Field(default=5432, description="DB host connection port")
    load_balance: Optional[str] = Field(default="False", description="Load balancing strategy")
    topology_keys: Optional[str] = Field(default="", description="Topology keys")
    yb_servers_refresh_interval: Optional[int] = Field(default=300, 
                                                       description="YB servers refresh interval")
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @contextmanager
    @requires_dependencies(["psycopg2"], extras="yugabytedb")
    def get_connection(self) -> Generator["YugabyteDbConnection", None, None]:
        from psycopg2 import connect

        access_config = self.access_config.get_secret_value()
        connection = connect(
            user=self.username,
            password=access_config.password,
            dbname=self.database,
            host=self.host,
            port=self.port,
            load_balance=self.load_balance,
            topology_keys=self.topology_keys,
            yb_servers_refresh_interval=self.yb_servers_refresh_interval,
        )
        try:
            yield connection
        finally:
            connection.commit()
            connection.close()

    @contextmanager
    def get_cursor(self) -> Generator["YugabyteDbCursor", None, None]:
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()


class YugabyteDbIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class YugabyteDbIndexer(SQLIndexer):
    connection_config: YugabyteDbConnectionConfig
    index_config: YugabyteDbIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class YugabyteDbDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class YugabyteDbDownloader(SQLDownloader):
    connection_config: YugabyteDbConnectionConfig
    download_config: YugabyteDbDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["psycopg2"], extras="yugabytedb")
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


class YugabyteDbUploadStagerConfig(SQLUploadStagerConfig):
    pass


class YugabyteDbUploadStager(SQLUploadStager):
    upload_stager_config: YugabyteDbUploadStagerConfig


class YugabyteDbUploaderConfig(SQLUploaderConfig):
    pass


@dataclass
class YugabyteDbUploader(SQLUploader):
    upload_config: YugabyteDbUploaderConfig = field(default_factory=YugabyteDbUploaderConfig)
    connection_config: YugabyteDbConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "%s"

    @requires_dependencies(["pandas"], extras="yugabytedb")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        super().run(path=path, file_data=file_data, **kwargs)


yugabytedb_source_entry = SourceRegistryEntry(
    connection_config=YugabyteDbConnectionConfig,
    indexer_config=YugabyteDbIndexerConfig,
    indexer=YugabyteDbIndexer,
    downloader_config=YugabyteDbDownloaderConfig,
    downloader=YugabyteDbDownloader,
)

yugabytedb_destination_entry = DestinationRegistryEntry(
    connection_config=YugabyteDbConnectionConfig,
    uploader=YugabyteDbUploader,
    uploader_config=YugabyteDbUploaderConfig,
    upload_stager=YugabyteDbUploadStager,
    upload_stager_config=YugabyteDbUploadStagerConfig,
)
