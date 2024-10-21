import hashlib
import json
import sys
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from pathlib import Path
from time import time
from typing import Any, Generator, Union

import pandas as pd
from dateutil import parser
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
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
    def get_connection(self) -> Any:
        pass


class SQLIndexerConfig(IndexerConfig):
    table_name: str
    id_column: str
    batch_size: int = 100


class SQLIndexer(Indexer, ABC):
    connection_config: SQLConnectionConfig
    index_config: SQLIndexerConfig

    @abstractmethod
    def _get_doc_ids(self) -> list[str]:
        pass

    def precheck(self) -> None:
        try:
            connection = self.connection_config.get_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT 1;")
            cursor.close()
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
        result.to_csv(download_path)
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
        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        output = []
        for data in elements_contents:
            metadata: dict[str, Any] = data.pop("metadata", {})
            data_source = metadata.pop("data_source", {})
            coordinates = metadata.pop("coordinates", {})

            data.update(metadata)
            data.update(data_source)
            data.update(coordinates)

            data["id"] = str(uuid.uuid4())

            # remove extraneous, not supported columns
            data = {k: v for k, v in data.items() if k in _COLUMNS}

            output.append(data)

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

        with output_path.open("w") as output_file:
            df.to_json(output_file, orient="records", lines=True)
        return output_path


class SQLUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="Number of records per batch")
    table_name: str = Field(default="elements", description="which table to upload contents to")


@dataclass
class SQLUploader(Uploader):
    upload_config: SQLUploaderConfig
    connection_config: SQLConnectionConfig

    def precheck(self) -> None:
        try:
            connection = self.connection_config.get_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT 1;")
            cursor.close()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @abstractmethod
    def prepare_data(
        self, columns: list[str], data: tuple[tuple[Any, ...], ...]
    ) -> list[tuple[Any, ...]]:
        pass

    @abstractmethod
    def upload_contents(self, path: Path) -> None:
        pass

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        self.upload_contents(path=path)
