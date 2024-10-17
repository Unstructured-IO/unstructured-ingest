import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Union

import pandas as pd
from dateutil import parser
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
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
