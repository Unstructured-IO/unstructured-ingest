import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generator, Optional

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
from unstructured_ingest.utils.data_prep import split_dataframe
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from pandas import DataFrame
    from teradatasql import TeradataConnection, TeradataCursor

CONNECTOR_TYPE = "teradata"


class TeradataAccessConfig(SQLAccessConfig):
    password: str = Field(description="Teradata user password")


class TeradataConnectionConfig(SQLConnectionConfig):
    access_config: Secret[TeradataAccessConfig]
    host: str = Field(description="Teradata server hostname or IP address")
    user: str = Field(description="Teradata database username")
    database: Optional[str] = Field(
        default=None,
        description="Default database/schema to use for queries",
    )
    dbs_port: int = Field(
        default=1025,
        description="Teradata database port (default: 1025)",
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @contextmanager
    @requires_dependencies(["teradatasql"], extras="teradata")
    def get_connection(self) -> Generator["TeradataConnection", None, None]:
        from teradatasql import connect

        conn_params = {
            "host": self.host,
            "user": self.user,
            "password": self.access_config.get_secret_value().password,
            "dbs_port": self.dbs_port,
        }
        if self.database:
            conn_params["database"] = self.database

        connection = connect(**conn_params)
        try:
            yield connection
        finally:
            connection.commit()
            connection.close()

    @contextmanager
    def get_cursor(self) -> Generator["TeradataCursor", None, None]:
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()


class TeradataIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class TeradataIndexer(SQLIndexer):
    connection_config: TeradataConnectionConfig
    index_config: TeradataIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def _get_doc_ids(self) -> list[str]:
        """Override to quote identifiers for Teradata reserved word handling."""
        with self.get_cursor() as cursor:
            cursor.execute(
                f'SELECT "{self.index_config.id_column}" FROM "{self.index_config.table_name}"'
            )
            results = cursor.fetchall()
            ids = sorted([result[0] for result in results])
            return ids


class TeradataDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class TeradataDownloader(SQLDownloader):
    connection_config: TeradataConnectionConfig
    download_config: TeradataDownloaderConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "?"

    def query_db(self, file_data: SqlBatchFileData) -> tuple[list[tuple], list[str]]:
        table_name = file_data.additional_metadata.table_name
        id_column = file_data.additional_metadata.id_column
        ids = [item.identifier for item in file_data.batch_items]

        with self.connection_config.get_cursor() as cursor:
            if self.download_config.fields:
                fields = ",".join([f'"{field}"' for field in self.download_config.fields])
            else:
                fields = "*"

            placeholders = ",".join([self.values_delimiter for _ in ids])
            query = f'SELECT {fields} FROM "{table_name}" WHERE "{id_column}" IN ({placeholders})'

            logger.debug(f"running query: {query}\nwith values: {ids}")
            cursor.execute(query, ids)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            return rows, columns


class TeradataUploadStagerConfig(SQLUploadStagerConfig):
    pass


@dataclass
class TeradataUploadStager(SQLUploadStager):
    upload_stager_config: TeradataUploadStagerConfig = field(
        default_factory=TeradataUploadStagerConfig
    )

    def conform_dataframe(self, df: "DataFrame") -> "DataFrame":
        df = super().conform_dataframe(df)

        # teradatasql driver cannot handle Python lists/dicts, convert to JSON strings
        # Check a sample of values to detect columns with complex types (10 rows)
        for column in df.columns:
            sample = df[column].dropna().head(10)
            
            if len(sample) > 0:
                has_complex_type = sample.apply(
                    lambda x: isinstance(x, (list, dict))
                ).any()
                
                if has_complex_type:
                    df[column] = df[column].apply(
                        lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                    )

        return df


class TeradataUploaderConfig(SQLUploaderConfig):
    pass


@dataclass
class TeradataUploader(SQLUploader):
    upload_config: TeradataUploaderConfig = field(default_factory=TeradataUploaderConfig)
    connection_config: TeradataConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "?"

    def get_table_columns(self) -> list[str]:
        if self._columns is None:
            with self.get_cursor() as cursor:
                cursor.execute(f'SELECT TOP 1 * FROM "{self.upload_config.table_name}"')
                self._columns = [desc[0] for desc in cursor.description]
        return self._columns

    def delete_by_record_id(self, file_data: FileData) -> None:
        logger.debug(
            f"deleting any content with data "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from table {self.upload_config.table_name}"
        )
        stmt = (
            f'DELETE FROM "{self.upload_config.table_name}" '
            f'WHERE "{self.upload_config.record_id_key}" = {self.values_delimiter}'
        )
        with self.get_cursor() as cursor:
            cursor.execute(stmt, [file_data.identifier])
            rowcount = cursor.rowcount
            if rowcount > 0:
                logger.info(f"deleted {rowcount} rows from table {self.upload_config.table_name}")

    def upload_dataframe(self, df: "DataFrame", file_data: FileData) -> None:
        import numpy as np

        if self.can_delete():
            self.delete_by_record_id(file_data=file_data)
        else:
            logger.warning(
                f"table doesn't contain expected "
                f"record id column "
                f"{self.upload_config.record_id_key}, skipping delete"
            )
        df = self._fit_to_schema(df=df)
        df.replace({np.nan: None}, inplace=True)

        columns = list(df.columns)
        quoted_columns = [f'"{col}"' for col in columns]

        stmt = "INSERT INTO {table_name} ({columns}) VALUES({values})".format(
            table_name=f'"{self.upload_config.table_name}"',
            columns=",".join(quoted_columns),
            values=",".join([self.values_delimiter for _ in columns]),
        )
        logger.info(
            f"writing a total of {len(df)} elements via"
            f" document batches to destination"
            f" table named {self.upload_config.table_name}"
            f" with batch size {self.upload_config.batch_size}"
        )
        for rows in split_dataframe(df=df, chunk_size=self.upload_config.batch_size):
            with self.get_cursor() as cursor:
                values = self.prepare_data(columns, tuple(rows.itertuples(index=False, name=None)))
                logger.debug(f"running query: {stmt}")
                cursor.executemany(stmt, values)


teradata_source_entry = SourceRegistryEntry(
    connection_config=TeradataConnectionConfig,
    indexer_config=TeradataIndexerConfig,
    indexer=TeradataIndexer,
    downloader_config=TeradataDownloaderConfig,
    downloader=TeradataDownloader,
)

teradata_destination_entry = DestinationRegistryEntry(
    connection_config=TeradataConnectionConfig,
    uploader=TeradataUploader,
    uploader_config=TeradataUploaderConfig,
    upload_stager=TeradataUploadStager,
    upload_stager_config=TeradataUploadStagerConfig,
)
