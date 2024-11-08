from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Optional

import numpy as np
import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.sql.postgres import (
    PostgresDownloader,
    PostgresDownloaderConfig,
    PostgresIndexer,
    PostgresIndexerConfig,
    PostgresUploader,
    PostgresUploaderConfig,
    PostgresUploadStager,
    PostgresUploadStagerConfig,
)
from unstructured_ingest.v2.processes.connectors.sql.sql import SQLAccessConfig, SQLConnectionConfig

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection
    from snowflake.connector.cursor import SnowflakeCursor

CONNECTOR_TYPE = "snowflake"


class SnowflakeAccessConfig(SQLAccessConfig):
    password: Optional[str] = Field(default=None, description="DB password")


class SnowflakeConnectionConfig(SQLConnectionConfig):
    access_config: Secret[SnowflakeAccessConfig] = Field(
        default=SnowflakeAccessConfig(), validate_default=True
    )
    account: str = Field(
        default=None,
        description="Your account identifier. The account identifier "
        "does not include the snowflakecomputing.com suffix.",
    )
    user: Optional[str] = Field(default=None, description="DB username")
    host: Optional[str] = Field(default=None, description="DB host")
    port: Optional[int] = Field(default=443, description="DB host connection port")
    database: str = Field(
        default=None,
        description="Database name.",
    )
    db_schema: str = Field(default=None, description="Database schema.", alias="schema")
    role: str = Field(
        default=None,
        description="Database role.",
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @contextmanager
    @requires_dependencies(["snowflake"], extras="snowflake")
    def get_connection(self) -> Generator["SnowflakeConnection", None, None]:
        # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#label-snowflake-connector-methods-connect
        from snowflake.connector import connect

        connect_kwargs = self.model_dump()
        connect_kwargs["schema"] = connect_kwargs.pop("db_schema")
        connect_kwargs.pop("access_configs", None)
        connect_kwargs["password"] = self.access_config.get_secret_value().password
        # https://peps.python.org/pep-0249/#paramstyle
        connect_kwargs["paramstyle"] = "qmark"
        # remove anything that is none
        active_kwargs = {k: v for k, v in connect_kwargs.items() if v is not None}
        connection = connect(**active_kwargs)
        try:
            yield connection
        finally:
            connection.commit()
            connection.close()

    @contextmanager
    def get_cursor(self) -> Generator["SnowflakeCursor", None, None]:
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()


class SnowflakeIndexerConfig(PostgresIndexerConfig):
    pass


@dataclass
class SnowflakeIndexer(PostgresIndexer):
    connection_config: SnowflakeConnectionConfig
    index_config: SnowflakeIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class SnowflakeDownloaderConfig(PostgresDownloaderConfig):
    pass


@dataclass
class SnowflakeDownloader(PostgresDownloader):
    connection_config: SnowflakeConnectionConfig
    download_config: SnowflakeDownloaderConfig
    connector_type: str = CONNECTOR_TYPE


class SnowflakeUploadStagerConfig(PostgresUploadStagerConfig):
    pass


class SnowflakeUploadStager(PostgresUploadStager):
    upload_stager_config: SnowflakeUploadStagerConfig


class SnowflakeUploaderConfig(PostgresUploaderConfig):
    pass


@dataclass
class SnowflakeUploader(PostgresUploader):
    upload_config: SnowflakeUploaderConfig = field(default_factory=SnowflakeUploaderConfig)
    connection_config: SnowflakeConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "?"

    def upload_contents(self, path: Path) -> None:
        df = pd.read_json(path, orient="records", lines=True)
        df.replace({np.nan: None}, inplace=True)

        columns = list(df.columns)
        stmt = f"INSERT INTO {self.upload_config.table_name} ({','.join(columns)}) VALUES({','.join([self.values_delimiter for x in columns])})"  # noqa E501

        for rows in pd.read_json(
            path, orient="records", lines=True, chunksize=self.upload_config.batch_size
        ):
            with self.connection_config.get_cursor() as cursor:
                values = self.prepare_data(columns, tuple(rows.itertuples(index=False, name=None)))
                # TODO: executemany break on 'Binding data in type (list) is not supported'
                for val in values:
                    cursor.execute(stmt, val)


snowflake_source_entry = SourceRegistryEntry(
    connection_config=SnowflakeConnectionConfig,
    indexer_config=SnowflakeIndexerConfig,
    indexer=SnowflakeIndexer,
    downloader_config=SnowflakeDownloaderConfig,
    downloader=SnowflakeDownloader,
)

snowflake_destination_entry = DestinationRegistryEntry(
    connection_config=SnowflakeConnectionConfig,
    uploader=SnowflakeUploader,
    uploader_config=SnowflakeUploaderConfig,
    upload_stager=SnowflakeUploadStager,
    upload_stager_config=SnowflakeUploadStagerConfig,
)
