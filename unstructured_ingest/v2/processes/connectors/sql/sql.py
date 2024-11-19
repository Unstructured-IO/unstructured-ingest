import hashlib
import json
import sys
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from pathlib import Path
from time import time
from typing import Any, Generator, Union

import numpy as np
import pandas as pd
from dateutil import parser
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
from unstructured_ingest.utils.data_prep import split_dataframe
from unstructured_ingest.v2.constants import RECORD_ID_LABEL
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
    download_responses,
)
from unstructured_ingest.v2.logger import logger

_COLUMNS = (
    "id",
    "element_id",
    "text",
    "embeddings",
    "type",
    "system",
    "layout_width",
    "layout_height",
    "points",
    "url",
    "version",
    "date_created",
    "date_modified",
    "date_processed",
    "permissions_data",
    "record_locator",
    "category_depth",
    "parent_id",
    "attached_filename",
    "filetype",
    "last_modified",
    "file_directory",
    "filename",
    "languages",
    "page_number",
    "links",
    "page_name",
    "link_urls",
    "link_texts",
    "sent_from",
    "sent_to",
    "subject",
    "section",
    "header_footer_type",
    "emphasized_text_contents",
    "emphasized_text_tags",
    "text_as_html",
    "regex_metadata",
    "detection_class_prob",
)

_DATE_COLUMNS = ("date_created", "date_modified", "date_processed", "last_modified")


def parse_date_string(date_value: Union[str, int]) -> date:
    try:
        timestamp = float(date_value) / 1000 if isinstance(date_value, int) else float(date_value)
        return datetime.fromtimestamp(timestamp)
    except Exception as e:
        logger.debug(f"date {date_value} string not a timestamp: {e}")
    return parser.parse(date_value)


class SQLAccessConfig(AccessConfig):
    pass


class SQLConnectionConfig(ConnectionConfig, ABC):
    access_config: Secret[SQLAccessConfig] = Field(default=SQLAccessConfig(), validate_default=True)

    @abstractmethod
    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        pass

    @abstractmethod
    @contextmanager
    def get_cursor(self) -> Generator[Any, None, None]:
        pass


class SQLIndexerConfig(IndexerConfig):
    table_name: str
    id_column: str
    batch_size: int = 100


class SQLIndexer(Indexer, ABC):
    connection_config: SQLConnectionConfig
    index_config: SQLIndexerConfig

    def _get_doc_ids(self) -> list[str]:
        with self.connection_config.get_cursor() as cursor:
            cursor.execute(
                f"SELECT {self.index_config.id_column} FROM {self.index_config.table_name}"
            )
            results = cursor.fetchall()
            ids = [result[0] for result in results]
            return ids

    def precheck(self) -> None:
        try:
            with self.connection_config.get_cursor() as cursor:
                cursor.execute("SELECT 1;")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        ids = self._get_doc_ids()
        id_batches: list[frozenset[str]] = [
            frozenset(
                ids[
                    i
                    * self.index_config.batch_size : (i + 1)  # noqa
                    * self.index_config.batch_size
                ]
            )
            for i in range(
                (len(ids) + self.index_config.batch_size - 1) // self.index_config.batch_size
            )
        ]
        for batch in id_batches:
            # Make sure the hash is always a positive number to create identified
            identified = str(hash(batch) + sys.maxsize + 1)
            yield FileData(
                identifier=identified,
                connector_type=self.connector_type,
                metadata=FileDataSourceMetadata(
                    date_processed=str(time()),
                ),
                doc_type="batch",
                additional_metadata={
                    "ids": list(batch),
                    "table_name": self.index_config.table_name,
                    "id_column": self.index_config.id_column,
                },
            )


class SQLDownloaderConfig(DownloaderConfig):
    fields: list[str] = field(default_factory=list)


class SQLDownloader(Downloader, ABC):
    connection_config: SQLConnectionConfig
    download_config: SQLDownloaderConfig

    @abstractmethod
    def query_db(self, file_data: FileData) -> tuple[list[tuple], list[str]]:
        pass

    def sql_to_df(self, rows: list[tuple], columns: list[str]) -> list[pd.DataFrame]:
        data = [dict(zip(columns, row)) for row in rows]
        df = pd.DataFrame(data)
        dfs = [pd.DataFrame([row.values], columns=df.columns) for index, row in df.iterrows()]
        return dfs

    def get_data(self, file_data: FileData) -> list[pd.DataFrame]:
        rows, columns = self.query_db(file_data=file_data)
        return self.sql_to_df(rows=rows, columns=columns)

    def get_identifier(self, table_name: str, record_id: str) -> str:
        f = f"{table_name}-{record_id}"
        if self.download_config.fields:
            f = "{}-{}".format(
                f,
                hashlib.sha256(",".join(self.download_config.fields).encode()).hexdigest()[:8],
            )
        return f

    def generate_download_response(
        self, result: pd.DataFrame, file_data: FileData
    ) -> DownloadResponse:
        id_column = file_data.additional_metadata["id_column"]
        table_name = file_data.additional_metadata["table_name"]
        record_id = result.iloc[0][id_column]
        filename_id = self.get_identifier(table_name=table_name, record_id=record_id)
        filename = f"{filename_id}.csv"
        download_path = self.download_dir / Path(filename)
        logger.debug(
            f"Downloading results from table {table_name} and id {record_id} to {download_path}"
        )
        download_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(download_path, index=False)
        copied_file_data = replace(file_data)
        copied_file_data.identifier = filename_id
        copied_file_data.doc_type = "file"
        copied_file_data.additional_metadata.pop("ids", None)
        return super().generate_download_response(
            file_data=copied_file_data, download_path=download_path
        )

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        data_dfs = self.get_data(file_data=file_data)
        download_responses = []
        for df in data_dfs:
            download_responses.append(
                self.generate_download_response(result=df, file_data=file_data)
            )
        return download_responses


class SQLUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class SQLUploadStager(UploadStager):
    upload_stager_config: SQLUploadStagerConfig = field(default_factory=SQLUploadStagerConfig)

    @staticmethod
    def conform_dict(data: dict, file_data: FileData) -> pd.DataFrame:
        working_data = data.copy()
        output = []
        for element in working_data:
            metadata: dict[str, Any] = element.pop("metadata", {})
            data_source = metadata.pop("data_source", {})
            coordinates = metadata.pop("coordinates", {})

            element.update(metadata)
            element.update(data_source)
            element.update(coordinates)

            element["id"] = str(uuid.uuid4())

            # remove extraneous, not supported columns
            element = {k: v for k, v in element.items() if k in _COLUMNS}
            element[RECORD_ID_LABEL] = file_data.identifier
            output.append(element)

        df = pd.DataFrame.from_dict(output)
        for column in filter(lambda x: x in df.columns, _DATE_COLUMNS):
            df[column] = df[column].apply(parse_date_string)
        for column in filter(
            lambda x: x in df.columns,
            ("permissions_data", "record_locator", "points", "links"),
        ):
            df[column] = df[column].apply(
                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None
            )
        for column in filter(
            lambda x: x in df.columns,
            ("version", "page_number", "regex_metadata"),
        ):
            df[column] = df[column].apply(str)
        return df

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents: list[dict] = json.load(elements_file)

        df = self.conform_dict(data=elements_contents, file_data=file_data)
        if Path(output_filename).suffix != ".json":
            output_filename = f"{output_filename}.json"
        else:
            output_filename = f"{Path(output_filename).stem}.json"
        output_path = Path(output_dir) / Path(f"{output_filename}")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open("w") as output_file:
            df.to_json(output_file, orient="records", lines=True)
        return output_path


class SQLUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="Number of records per batch")
    table_name: str = Field(default="elements", description="which table to upload contents to")
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="searchable key to find entries for the same record on previous runs",
    )


@dataclass
class SQLUploader(Uploader):
    upload_config: SQLUploaderConfig
    connection_config: SQLConnectionConfig
    values_delimiter: str = "?"

    def precheck(self) -> None:
        try:
            with self.connection_config.get_cursor() as cursor:
                cursor.execute("SELECT 1;")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

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

    def _fit_to_schema(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        columns = set(df.columns)
        schema_fields = set(columns)
        columns_to_drop = columns - schema_fields
        missing_columns = schema_fields - columns

        if columns_to_drop:
            logger.warning(
                "Following columns will be dropped to match the table's schema: "
                f"{', '.join(columns_to_drop)}"
            )
        if missing_columns:
            logger.info(
                "Following null filled columns will be added to match the table's schema:"
                f" {', '.join(missing_columns)} "
            )

        df = df.drop(columns=columns_to_drop)

        for column in missing_columns:
            df[column] = pd.Series()

    def upload_contents(self, path: Path) -> None:
        df = pd.read_json(path, orient="records", lines=True)
        df.replace({np.nan: None}, inplace=True)
        self._fit_to_schema(df=df, columns=self.get_table_columns())

        columns = list(df.columns)
        stmt = f"INSERT INTO {self.upload_config.table_name} ({','.join(columns)}) VALUES({','.join([self.values_delimiter for x in columns])})"  # noqa E501
        logger.info(
            f"writing a total of {len(df)} elements via"
            f" document batches to destination"
            f" table named {self.upload_config.table_name}"
            f" with batch size {self.upload_config.batch_size}"
        )
        for rows in split_dataframe(df=df, chunk_size=self.upload_config.batch_size):
            with self.connection_config.get_cursor() as cursor:
                values = self.prepare_data(columns, tuple(rows.itertuples(index=False, name=None)))
                # For debugging purposes:
                # for val in values:
                #     try:
                #         cursor.execute(stmt, val)
                #     except Exception as e:
                #         print(f"Error: {e}")
                #         print(f"failed to write {len(columns)}, {len(val)}: {stmt} -> {val}")
                cursor.executemany(stmt, values)

    def get_table_columns(self) -> list[str]:
        with self.connection_config.get_cursor() as cursor:
            cursor.execute(f"SELECT * from {self.upload_config.table_name}")
            return [desc[0] for desc in cursor.description]

    def can_delete(self) -> bool:
        return self.upload_config.record_id_key in self.get_table_columns()

    def delete_by_record_id(self, file_data: FileData) -> None:
        logger.debug(
            f"deleting any content with data "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from table {self.upload_config.table_name}"
        )
        stmt = f"DELETE FROM {self.upload_config.table_name} WHERE {self.upload_config.record_id_key} = {self.values_delimiter}"  # noqa: E501
        with self.connection_config.get_cursor() as cursor:
            cursor.execute(stmt, [file_data.identifier])
            rowcount = cursor.rowcount
            logger.info(f"deleted {rowcount} rows from table {self.upload_config.table_name}")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        if self.can_delete():
            self.delete_by_record_id(file_data=file_data)
        else:
            logger.warning(
                f"table doesn't contain expected "
                f"record id column "
                f"{self.upload_config.record_id_key}, skipping delete"
            )
        self.upload_contents(path=path)
