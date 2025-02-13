import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generator, Optional

import numpy as np
import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.utils.data_prep import split_dataframe
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces.file_data import FileData
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.sql.sql import (
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

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection
    from snowflake.connector.cursor import SnowflakeCursor

CONNECTOR_TYPE = "snowflake"

_ARRAY_COLUMNS = (
    "embeddings",
    "languages",
    "link_urls",
    "link_texts",
    "sent_from",
    "sent_to",
    "emphasized_text_contents",
    "emphasized_text_tags",
)


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
    # The actual snowflake module package name is: snowflake-connector-python
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


class SnowflakeIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class SnowflakeIndexer(SQLIndexer):
    connection_config: SnowflakeConnectionConfig
    index_config: SnowflakeIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class SnowflakeDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class SnowflakeDownloader(SQLDownloader):
    connection_config: SnowflakeConnectionConfig
    download_config: SnowflakeDownloaderConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "?"

    # The actual snowflake module package name is: snowflake-connector-python
    @requires_dependencies(["snowflake"], extras="snowflake")
    def query_db(self, file_data: SqlBatchFileData) -> tuple[list[tuple], list[str]]:
        table_name = file_data.additional_metadata.table_name
        id_column = file_data.additional_metadata.id_column
        ids = [item.identifier for item in file_data.batch_items]

        with self.connection_config.get_cursor() as cursor:
            query = "SELECT {fields} FROM {table_name} WHERE {id_column} IN ({values})".format(
                table_name=table_name,
                id_column=id_column,
                fields=(
                    ",".join(self.download_config.fields) if self.download_config.fields else "*"
                ),
                values=",".join([self.values_delimiter for _ in ids]),
            )
            logger.debug(f"running query: {query}\nwith values: {ids}")
            cursor.execute(query, ids)
            rows = [
                tuple(row.values()) if isinstance(row, dict) else row for row in cursor.fetchall()
            ]
            columns = [col[0] for col in cursor.description]
            return rows, columns


class SnowflakeUploadStagerConfig(SQLUploadStagerConfig):
    pass


class SnowflakeUploadStager(SQLUploadStager):
    upload_stager_config: SnowflakeUploadStagerConfig


class SnowflakeUploaderConfig(SQLUploaderConfig):
    pass


@dataclass
class SnowflakeUploader(SQLUploader):
    upload_config: SnowflakeUploaderConfig = field(default_factory=SnowflakeUploaderConfig)
    connection_config: SnowflakeConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "?"

    def prepare_data(
        self, columns: list[str], data: tuple[tuple[Any, ...], ...]
    ) -> list[tuple[Any, ...]]:
        output = []
        for row in data:
            parsed = []
            for column_name, value in zip(columns, row):
                if column_name in _DATE_COLUMNS:
                    if value is None or pd.isna(value):  # pandas is nan
                        parsed.append(None)
                    else:
                        parsed.append(parse_date_string(value))
                elif column_name in _ARRAY_COLUMNS:
                    if not isinstance(value, list) and (
                        value is None or pd.isna(value)
                    ):  # pandas is nan
                        parsed.append(None)
                    else:
                        parsed.append(json.dumps(value))
                else:
                    parsed.append(value)
            output.append(tuple(parsed))
        return output

    def _parse_values(self, columns: list[str]) -> str:
        return ",".join(
            [
                (
                    f"PARSE_JSON({self.values_delimiter})"
                    if col in _ARRAY_COLUMNS
                    else self.values_delimiter
                )
                for col in columns
            ]
        )

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
        stmt = "INSERT INTO {table_name} ({columns}) SELECT {values}".format(
            table_name=self.upload_config.table_name,
            columns=",".join(columns),
            values=self._parse_values(columns),
        )
        logger.info(
            f"writing a total of {len(df)} elements via"
            f" document batches to destination"
            f" table named {self.upload_config.table_name}"
            f" with batch size {self.upload_config.batch_size}"
        )
        for rows in split_dataframe(df=df, chunk_size=self.upload_config.batch_size):
            with self.connection_config.get_cursor() as cursor:
                values = self.prepare_data(columns, tuple(rows.itertuples(index=False, name=None)))
                # TODO: executemany break on 'Binding data in type (list) is not supported'
                for val in values:
                    logger.debug(f"running query: {stmt}\nwith values: {val}")
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
