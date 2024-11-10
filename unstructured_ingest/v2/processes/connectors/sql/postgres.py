from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import numpy as np
import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import FileData
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry
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
    from psycopg2.extensions import connection as PostgresConnection

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

    @requires_dependencies(["psycopg2"], extras="postgres")
    def get_connection(self) -> "PostgresConnection":
        from psycopg2 import connect

        access_config = self.access_config.get_secret_value()
        return connect(
            user=self.username,
            password=access_config.password,
            dbname=self.database,
            host=self.host,
            port=self.port,
        )


class PostgresIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class PostgresIndexer(SQLIndexer):
    connection_config: PostgresConnectionConfig
    index_config: PostgresIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def _get_doc_ids(self) -> list[str]:
        connection = self.connection_config.get_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {self.index_config.id_column} FROM {self.index_config.table_name}"
            )
            results = cursor.fetchall()
            ids = [result[0] for result in results]
            return ids


class PostgresDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class PostgresDownloader(SQLDownloader):
    connection_config: PostgresConnectionConfig
    download_config: PostgresDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    def query_db(self, file_data: FileData) -> tuple[list[tuple], list[str]]:
        table_name = file_data.additional_metadata["table_name"]
        id_column = file_data.additional_metadata["id_column"]
        ids = file_data.additional_metadata["ids"]
        connection = self.connection_config.get_connection()
        with connection.cursor() as cursor:
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

    def prepare_data(
        self, columns: list[str], data: tuple[tuple[Any, ...], ...]
    ) -> list[tuple[Any, ...]]:
        output = []
        for row in data:
            parsed = []
            for column_name, value in zip(columns, row):
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
        stmt = f"INSERT INTO {self.upload_config.table_name} ({','.join(columns)}) \
                VALUES({','.join(['%s' for x in columns])})"  # noqa E501

        for rows in pd.read_json(
            path, orient="records", lines=True, chunksize=self.upload_config.batch_size
        ):
            with self.connection_config.get_connection() as conn:
                values = self.prepare_data(columns, tuple(rows.itertuples(index=False, name=None)))
                with conn.cursor() as cur:
                    cur.executemany(stmt, values)

                conn.commit()


postgres_destination_entry = DestinationRegistryEntry(
    connection_config=PostgresConnectionConfig,
    uploader=PostgresUploader,
    uploader_config=PostgresUploaderConfig,
    upload_stager=PostgresUploadStager,
    upload_stager_config=PostgresUploadStagerConfig,
)
