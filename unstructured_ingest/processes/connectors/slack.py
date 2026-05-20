import asyncio
import hashlib
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError, ValueError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import SourceRegistryEntry
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from slack_sdk import WebClient
    from slack_sdk.web.async_client import AsyncWebClient

# NOTE: Pagination limit set to the upper end of the recommended range
# https://api.slack.com/apis/pagination#facts
PAGINATION_LIMIT = 200

CONNECTOR_TYPE = "slack"


def _safe_slack_filename(filename: str) -> str:
    sanitized = re.sub(r"[/\\]+", "_", filename).strip()
    return sanitized or "slack-file"


class SlackAccessConfig(AccessConfig):
    token: str = Field(
        description="Bot token used to access Slack API, must have channels:history scope for the"
        " bot user."
    )
    refresh_token: Optional[str] = Field(default=None, description="Slack OAuth refresh token.")


class SlackConnectionConfig(ConnectionConfig):
    access_config: Secret[SlackAccessConfig]

    @requires_dependencies(["slack_sdk"], extras="slack")
    @SourceConnectionError.wrap
    def get_client(self) -> "WebClient":
        from slack_sdk import WebClient

        return WebClient(token=self.access_config.get_secret_value().token)

    @requires_dependencies(["slack_sdk"], extras="slack")
    @SourceConnectionError.wrap
    def get_async_client(self) -> "AsyncWebClient":
        from slack_sdk.web.async_client import AsyncWebClient

        return AsyncWebClient(token=self.access_config.get_secret_value().token)


class SlackIndexerConfig(IndexerConfig):
    channels: list[str] = Field(
        description="Comma-delimited list of Slack channel IDs to pull messages from, can be"
        " both public or private channels."
    )
    start_date: Optional[datetime] = Field(
        default=None,
        description="Start date/time in formats YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]"
        " or YYYY-MM-DD",
    )
    end_date: Optional[datetime] = Field(
        default=None,
        description="End date/time in formats YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]"
        " or YYYY-MM-DD",
    )


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
                str(self.index_config.start_date.timestamp())
                if self.index_config.start_date is not None
                else None
            )
            latest = (
                str(self.index_config.end_date.timestamp())
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
                    yield self._messages_to_file_data(messages, channel)
                    for file_data in self._message_files_to_file_data(messages, channel):
                        yield file_data

    def _messages_to_file_data(
        self,
        messages: list[dict],
        channel: str,
    ) -> FileData:
        ts_oldest = min((message["ts"] for message in messages), key=lambda m: float(m))
        ts_newest = max((message["ts"] for message in messages), key=lambda m: float(m))

        identifier_base = f"{channel}-{ts_oldest}-{ts_newest}"
        identifier = hashlib.sha256(identifier_base.encode("utf-8")).hexdigest()
        filename = identifier[:16]

        source_identifiers = SourceIdentifiers(
            filename=f"{filename}.xml", fullpath=f"{filename}.xml"
        )
        return FileData(
            identifier=identifier,
            connector_type=CONNECTOR_TYPE,
            source_identifiers=source_identifiers,
            metadata=FileDataSourceMetadata(
                date_created=ts_oldest,
                date_modified=ts_newest,
                date_processed=str(time.time()),
                record_locator={
                    "channel": channel,
                    "oldest": ts_oldest,
                    "latest": ts_newest,
                },
            ),
            display_name=source_identifiers.fullpath,
        )

    def _message_files_to_file_data(
        self,
        messages: list[dict],
        channel: str,
    ) -> Generator[FileData, None, None]:
        for message in messages:
            message_ts = message.get("ts")
            for slack_file in message.get("files", []) or []:
                file_id = slack_file.get("id")
                if not file_id or not message_ts:
                    continue

                filename = _safe_slack_filename(
                    f"{file_id}-{slack_file.get('name') or slack_file.get('title') or file_id}"
                )
                identifier_base = f"{channel}-{message_ts}-{file_id}"
                identifier = hashlib.sha256(identifier_base.encode("utf-8")).hexdigest()
                source_identifiers = SourceIdentifiers(filename=filename, fullpath=filename)
                yield FileData(
                    identifier=identifier,
                    connector_type=CONNECTOR_TYPE,
                    source_identifiers=source_identifiers,
                    metadata=FileDataSourceMetadata(
                        date_created=(
                            str(slack_file.get("created")) if slack_file.get("created") else None
                        ),
                        date_modified=message_ts,
                        date_processed=str(time.time()),
                        record_locator={
                            "type": "file",
                            "channel": channel,
                            "message_ts": message_ts,
                            "file_id": file_id,
                        },
                    ),
                    display_name=source_identifiers.fullpath,
                )

    @SourceConnectionError.wrap
    def precheck(self) -> None:
        client = self.connection_config.get_client()
        for channel in self.index_config.channels:
            # NOTE: Querying conversations history guarantees that the bot is in the channel
            client.conversations_history(channel=channel, limit=1)


class SlackDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class SlackDownloader(Downloader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: SlackConnectionConfig
    download_config: SlackDownloaderConfig = field(default_factory=SlackDownloaderConfig)

    def run(self, file_data, **kwargs):
        raise NotImplementedError

    async def run_async(self, file_data: FileData, **kwargs) -> DownloadResponse:
        # NOTE: Indexer should provide source identifiers required to generate the download path
        download_path = self.get_download_path(file_data)
        if download_path is None:
            logger.error(
                "Generated download path is None, source_identifiers might be missingfrom FileData."
            )
            raise ValueError("Generated invalid download path.")

        if (
            file_data.metadata.record_locator
            and file_data.metadata.record_locator.get("type") == "file"
        ):
            await self._download_file(file_data, download_path)
        else:
            await self._download_conversation(file_data, download_path)
        return self.generate_download_response(file_data, download_path)

    def is_async(self):
        return True

    async def _download_conversation(self, file_data: FileData, download_path: Path) -> None:
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

        client = self.connection_config.get_async_client()
        messages = []
        async for conversation_history in await client.conversations_history(
            channel=file_data.metadata.record_locator["channel"],
            oldest=file_data.metadata.record_locator["oldest"],
            latest=file_data.metadata.record_locator["latest"],
            limit=PAGINATION_LIMIT,
            # NOTE: In order to get the exact same range of messages as indexer, it provides
            # timestamps of oldest and newest messages, inclusive=True is necessary to include them
            inclusive=True,
        ):
            messages += conversation_history.get("messages", [])

        conversation = []
        for message in messages:
            thread_messages = []
            async for conversations_replies in await client.conversations_replies(
                channel=file_data.metadata.record_locator["channel"],
                ts=message["ts"],
                limit=PAGINATION_LIMIT,
            ):
                thread_messages += conversations_replies.get("messages", [])

            # NOTE: Replies contains the whole thread, including the message references by the `ts`
            # parameter even if it's the only message (there were no replies).
            # Reference: https://api.slack.com/methods/conversations.replies#markdown
            conversation.append(thread_messages)

        conversation_xml = self._conversation_to_xml(conversation)
        download_path.parent.mkdir(exist_ok=True, parents=True)
        conversation_xml.write(download_path, encoding="utf-8", xml_declaration=True)

    async def _download_file(self, file_data: FileData, download_path: Path) -> None:
        record_locator = file_data.metadata.record_locator
        if record_locator is None or "file_id" not in record_locator:
            logger.error(f"Invalid file record locator in metadata: {record_locator}.")
            raise ValueError("Invalid file record locator.")

        client = self.connection_config.get_async_client()
        file_info = await client.files_info(file=record_locator["file_id"])
        if not file_info.get("ok", True):
            raise ValueError(f"Slack files.info failed: {file_info.get('error')}")

        slack_file = file_info.get("file", {})
        download_url = slack_file.get("url_private_download") or record_locator.get(
            "url_private_download"
        )
        if not download_url:
            raise ValueError("Slack file is missing url_private_download.")

        token = self.connection_config.access_config.get_secret_value().token
        request = urllib.request.Request(
            download_url,
            headers={"Authorization": f"Bearer {token}"},
        )
        download_path.parent.mkdir(exist_ok=True, parents=True)
        content = await asyncio.to_thread(self._read_private_file, request)
        download_path.write_bytes(content)

    @staticmethod
    def _read_private_file(request: urllib.request.Request) -> bytes:
        with urllib.request.urlopen(request) as response:
            return response.read()

    def _conversation_to_xml(self, conversation: list[list[dict]]) -> ET.ElementTree:
        root = ET.Element("messages")

        for thread in conversation:
            message, *replies = thread
            message_elem = ET.SubElement(root, "message")
            text_elem = ET.SubElement(message_elem, "text")
            text_elem.text = message.get("text")

            for reply in replies:
                reply_msg = reply.get("text", "")
                text_elem.text = "".join([str(text_elem.text), " <reply> ", reply_msg])

        return ET.ElementTree(root)


slack_source_entry = SourceRegistryEntry(
    indexer=SlackIndexer,
    indexer_config=SlackIndexerConfig,
    downloader=SlackDownloader,
    downloader_config=DownloaderConfig,
    connection_config=SlackConnectionConfig,
)
