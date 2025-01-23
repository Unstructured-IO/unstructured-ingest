from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generator, Optional

from pydantic import Field, Secret
import numpy as np
import pandas as pd
import pyarrow as pa
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
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
from unstructured_ingest.v2.interfaces import (
    FileData,
)
from unstructured_ingest.utils.data_prep import get_data, get_data_df, split_dataframe

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
    # database: Optional[str] = Field(
    #     default=None,
    #     description="Database name.",
    # )
    vastdb_bucket: str
    vastdb_schema: str
    vastdb_table: str
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    # @contextmanager
    @requires_dependencies(["vastdb","ibis","pyarrow"], extras="vastdb")
    def get_connection(self) -> Generator["VastdbConnect", None, None]:
        from vastdb import connect

        access_config = self.access_config.get_secret_value()
        connection = connect(
            endpoint=access_config.endpoint,
            access=access_config.access_key_id,
            secret=access_config.access_key_secret,

        )
        return connection
        # try:
        #     yield connection
        # finally:
        #     return
        #     # connection.commit()
        #     # connection.close()

    # @contextmanager
    def get_cursor(self) -> Generator["VastdbTransaction", None, None]:
        # with self.get_connection() as connection:
        logger.info("GET CURSOR!!!")

        cursor = self.get_connection().transaction()
        return cursor
        # breakpoint()
        # try:
        #     yield cursor
        # finally:
        #     return
        #         # cursor.close()


class VastdbIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class VastdbIndexer(SQLIndexer):
    connection_config: VastdbConnectionConfig
    index_config: VastdbIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def _get_doc_ids(self) -> list[str]:
        with self.get_cursor() as cursor:
            bucket = cursor.bucket(self.connection_config.vastdb_bucket)
            schema = bucket.schema(self.connection_config.vastdb_schema)
            table = schema.table(self.index_config.table_name)
            reader = table.select(columns=[self.index_config.id_column])
            results = reader.read_all()  # Build a PyArrow Table from the RecordBatchReader

            # cursor.execute(
            #     f"SELECT {self.index_config.id_column} FROM {self.index_config.table_name}"
            # )
            # results = cursor.fetchall()
            ids = sorted([result[self.index_config.id_column] for result in results.to_pylist()])
            return ids

    def precheck(self) -> None:
        try:
            with self.get_cursor() as cursor:
                bucket = cursor.bucket(self.connection_config.vastdb_bucket)
                logger.info(bucket.schemas())
                schema = bucket.schema(self.connection_config.vastdb_schema)
                table = schema.table(self.index_config.table_name)
                # cursor.execute("SELECT 1;")
                table.select()
                logger.info("PRECHECK PASSED !!!!!!!!!!!!!!!!!!!")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")


class VastdbDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class VastdbDownloader(SQLDownloader):
    connection_config: VastdbConnectionConfig
    download_config: VastdbDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["vastdb","ibis","pyarrow"], extras="vastdb")
    def query_db(self, file_data: SqlBatchFileData) -> tuple[list[tuple], list[str]]:
        from ibis.expr import sql

        table_name = file_data.additional_metadata.table_name
        id_column = file_data.additional_metadata.id_column
        ids = tuple([item.identifier for item in file_data.batch_items])
        breakpoint()

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

    def precheck(self) -> None:
        try:
            with self.get_cursor() as cursor:
                bucket = cursor.bucket(self.connection_config.vastdb_bucket)
                logger.info(bucket.schemas())
                schema = bucket.schema(self.connection_config.vastdb_schema)
                table = schema.table(self.upload_config.table_name)
                # cursor.execute("SELECT 1;")
                table.select()
                logger.info("PRECHECK PASSED !!!!!!!!!!!!!!!!!!!")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def upload_dataframe(self, df: pd.DataFrame, file_data: FileData) -> None:
        if self.can_delete():
            self.delete_by_record_id(file_data=file_data)
        else:
            logger.warning(
                f"table doesn't contain expected "
                f"record id column "
                f"{self.upload_config.record_id_key}, skipping delete"
            )
        df.replace({np.nan: None}, inplace=True)
        self._fit_to_schema(df=df)

        columns = list(df.columns)
        logger.info("ABOUT TO INSERT !!!!!!!")
        stmt = "INSERT INTO {table_name} ({columns}) VALUES({values})".format(
            table_name=self.upload_config.table_name,
            columns=",".join(columns),
            values=",".join([self.values_delimiter for _ in columns]),
        )
        logger.info(
            f"writing a total of {len(df)} elements via"
            f" document batches to destination"
            f" table named {self.upload_config.table_name}"
            f" with batch size {self.upload_config.batch_size}"
        )
        df.drop(['languages', 'date_created', 'date_modified','date_processed','parent_id'], axis=1, inplace=True, errors='ignore')

        for rows in split_dataframe(df=df, chunk_size=self.upload_config.batch_size):

            with self.get_cursor() as cursor:
                # values = self.prepare_data(columns, tuple(rows.itertuples(index=False, name=None)))
                # logger.info(values)
            
                # For debugging purposes:
                # for val in values:
                #     try:
                #         cursor.execute(stmt, val)
                #     except Exception as e:
                #         print(f"Error: {e}")
                #         print(f"failed to write {len(columns)}, {len(val)}: {stmt} -> {val}")
                # logger.debug(f"running query: {stmt}")
                # cursor.executemany(stmt, values)
                pa_table = pa.Table.from_pandas(df)
                bucket = cursor.bucket(self.connection_config.vastdb_bucket)
                schema = bucket.schema(self.connection_config.vastdb_schema)
                table = schema.table(self.connection_config.vastdb_table)
                table.insert(pa_table)

    def can_delete(self) -> bool:
        return False

vastdb_source_entry = SourceRegistryEntry(
    connection_config=VastdbConnectionConfig,
    indexer_config=VastdbIndexerConfig,
    indexer=VastdbIndexer,
    downloader_config=VastdbDownloaderConfig,
    downloader=VastdbDownloader,
)

vastdb_destination_entry = DestinationRegistryEntry(
    connection_config=VastdbConnectionConfig,
    uploader=VastdbUploader,
    uploader_config=VastdbUploaderConfig,
    upload_stager=VastdbUploadStager,
    upload_stager_config=VastdbUploadStagerConfig,
)