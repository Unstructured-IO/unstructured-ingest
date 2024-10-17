import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret, field_validator

from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.data_prep import validate_date_args
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    Indexer,
    IndexerConfig,
    download_responses,
)
from unstructured_ingest.v2.interfaces.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

if TYPE_CHECKING:
    # TODO: Move to async client
    # from slack_sdk.web.async_client import AsyncWebClient
    from slack_sdk import WebClient

# Pagination limit set to the upper end of the recommended range
# https://api.slack.com/apis/pagination#facts
PAGINATION_LIMIT = 200
SUPPORTED_DATE_FORMATS = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z")

CONNECTOR_TYPE = "slack"


class SlackAccessConfig(AccessConfig):
    token: str = Field(description="")  # TODO: Describe


class SlackConnectionConfig(ConnectionConfig):
    access_config: Secret[SlackAccessConfig] = Field(description="")  # TODO: Describe

    @requires_dependencies(["slack_sdk"], extras="slack")
    @SourceConnectionError.wrap
    def get_client(self) -> "WebClient":
        from slack_sdk import WebClient

        return WebClient(token=self.access_config.get_secret_value().token)


class SlackIndexerConfig(IndexerConfig):
    channels: list[str] = Field(description="")  # TODO: Describe
    start_date: Optional[str] = Field(description="")
    end_date: Optional[str] = Field(description="")

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_datetime(cls, datetime: Optional[str]) -> Optional[str]:
        if datetime is None or validate_date_args(datetime):
            return datetime
        raise ValueError("Invalid datetime format")


@dataclass
class SlackIndexer(Indexer):
    index_config: SlackIndexerConfig
    connection_config: SlackConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        client = self.connection_config.get_client()
        for channel in self.index_config.channels:
            messages = []
            oldest = (
                str(_convert_datetime(self.index_config.start_date))
                if self.index_config.start_date is not None
                else None
            )
            latest = (
                str(_convert_datetime(self.index_config.end_date))
                if self.index_config.end_date is not None
                else None
            )
            for conversation_history in client.conversations_history(
                channel=channel,
                oldest=oldest,
                latest=latest,
                limit=PAGINATION_LIMIT,
            ):
                messages = conversation_history.get("messages", [])
                if messages:
                    yield self._messages_to_file_data(messages, channel, oldest, latest)

    def is_async(self):
        return False

    def _messages_to_file_data(
        self,
        messages: list[dict],
        channel: str,
        oldest: Optional[str],
        latest: Optional[str],
    ) -> FileData:
        date_created = str(min(float(message["ts"]) for message in messages))
        date_modified = str(max(float(message["ts"]) for message in messages))

        return FileData(
            # NOTE: This isn't quite unique in this way, could consider adding
            # a hash of earliest and latest timestamps?
            identifier=channel,
            connector_type=CONNECTOR_TYPE,
            source_identifiers=SourceIdentifiers(
                filename=f"{channel}.xml", fullpath=f"{channel}.xml"
            ),
            metadata=FileDataSourceMetadata(
                date_created=date_created,
                date_modified=date_modified,
                date_processed=str(time.time()),
                record_locator={
                    "channel": channel,
                    "oldest": oldest,
                    "latest": latest,
                },
            ),
        )

    @SourceConnectionError.wrap
    def precheck(self) -> None:
        client = self.connection_config.get_client()
        for channel in self.index_config.channels:
            client.conversations_info(channel=channel)


class SlackDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class SlackDownloader(Downloader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: SlackConnectionConfig
    download_config: SlackDownloaderConfig = field(default_factory=SlackDownloaderConfig)

    def run(self, file_data: FileData, **kwargs) -> download_responses:
        # NOTE: Indexer should provide source identifiers required to generate the download path
        download_path = self.get_download_path(file_data)
        if download_path is None:
            logger.error(
                "Generated download path is None, source_identifiers might be missing"
                "from FileData."
            )
            raise ValueError("Generated invalid download path.")

        self._download_conversation(file_data, download_path)
        return self.generate_download_response(file_data, download_path)

    def _download_conversation(self, file_data: FileData, download_path: Path) -> None:
        # NOTE: Indexer should supply the record locator in metadata
        if (
            file_data.metadata.record_locator is None
            or "channel" not in file_data.metadata.record_locator
            or "oldest" not in file_data.metadata.record_locator
            or "latest" not in file_data.metadata.record_locator
        ):
            logger.error(
                f"Invalid record locator in metadata: {file_data.metadata.record_locator}."
                "Keys 'channel', 'oldest' and 'latest' must be present."
            )
            raise ValueError("Invalid record locator.")

        client = self.connection_config.get_client()
        messages = []
        for conversation_history in client.conversations_history(
            channel=file_data.metadata.record_locator["channel"],
            oldest=file_data.metadata.record_locator["oldest"],
            latest=file_data.metadata.record_locator["latest"],
            limit=PAGINATION_LIMIT,
        ):
            messages += conversation_history.get("messages", [])

        messages_with_replies = []
        for message in messages:
            # NOTE: First reply is the original message
            replies = []
            for conversations_replies in client.conversations_replies(
                channel=file_data.metadata.record_locator["channel"],
                ts=message["ts"],
                limit=PAGINATION_LIMIT,
            ):
                replies += conversations_replies.get("messages", [])
            messages_with_replies.append((message, replies))

        conversation_xml = self._messages_with_replies_to_xml(messages_with_replies)
        download_path.parent.mkdir(exist_ok=True, parents=True)
        conversation_xml.write(download_path, encoding="utf-8", xml_declaration=True)

    def _messages_with_replies_to_xml(
        self, messages_with_replies: list[tuple[dict, list[dict]]]
    ) -> ET.ElementTree:
        root = ET.Element("messages")
        for message, replies in messages_with_replies:
            message_elem = ET.SubElement(root, "message")
            text_elem = ET.SubElement(message_elem, "text")
            text_elem.text = message.get("text")

            for reply in replies:
                reply_msg = reply.get("text", "")
                text_elem.text = "".join([str(text_elem.text), " <reply> ", reply_msg])

        return ET.ElementTree(root)


def _convert_datetime(date_time: str) -> float:
    for format in SUPPORTED_DATE_FORMATS:
        try:
            return datetime.strptime(date_time, format).timestamp()
        except ValueError:
            pass
    raise ValueError("Invalid datetime format")


slack_source_entry = SourceRegistryEntry(
    indexer=SlackIndexer,
    indexer_config=SlackIndexerConfig,
    downloader=SlackDownloader,
    downloader_config=DownloaderConfig,
    connection_config=SlackConnectionConfig,
)
