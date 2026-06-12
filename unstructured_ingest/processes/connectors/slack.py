import asyncio
import builtins
import hashlib
import re
import shutil
import time
import urllib.parse
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
from unstructured_ingest.error import (
    SourceConnectionError,
    UnstructuredIngestError,
    ValueError,
)
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
PRIVATE_FILE_DOWNLOAD_TIMEOUT_SECONDS = 60
SLACK_PRIVATE_FILE_HOST = "files.slack.com"

CONNECTOR_TYPE = "slack"

# Slack API error codes that mean the bot cannot read the channel because it is not a
# member of it. These are turned into a clear, per-channel, user-facing error.
CHANNEL_MEMBERSHIP_ERROR_CODES = frozenset({"not_in_channel", "channel_not_found"})


def _slack_api_error_code(error: BaseException) -> Optional[str]:
    """Best-effort extraction of a Slack API error code (e.g. ``not_in_channel``).

    Returns ``None`` for any exception that is not a recognizable Slack API error so the
    caller can fall back to generic error handling.
    """
    response = getattr(error, "response", None)
    if response is None:
        return None
    getter = getattr(response, "get", None)
    if callable(getter):
        code = getter("error")
        if isinstance(code, str):
            return code
    return None


def _safe_slack_filename(filename: str) -> str:
    sanitized = re.sub(r"[/\\]+", "_", filename).strip()
    return sanitized or "slack-file"


def _validate_private_download_url(download_url: str) -> str:
    parsed_url = urllib.parse.urlparse(download_url)
    hostname = parsed_url.hostname.lower() if parsed_url.hostname else None

    if parsed_url.scheme != "https" or hostname != SLACK_PRIVATE_FILE_HOST:
        raise ValueError("Slack file download URL must be an HTTPS files.slack.com URL.")

    if parsed_url.username or parsed_url.password:
        raise ValueError("Slack file download URL must not include credentials.")

    try:
        port = parsed_url.port
    except builtins.ValueError as exc:
        raise ValueError("Slack file download URL has an invalid port.") from exc

    if port not in (None, 443):
        raise ValueError("Slack file download URL must use the default HTTPS port.")

    return download_url


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102, ANN001
        raise ValueError(
            "Slack file download redirected; refusing to forward bearer authorization."
        )


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
            # Auto-join public channels (and detect private ones) before reading so the
            # bot is a member, mirroring precheck().
            is_private = self._prepare_channel_access(client, channel)
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
            # NOTE: run() is a generator, so SourceConnectionError.wrap (used by precheck())
            # only guards generator creation and cannot translate errors raised during
            # iteration. Translate inline so run()'s errors surface like precheck()'s.
            try:
                for conversation_history in client.conversations_history(
                    channel=channel,
                    oldest=oldest,
                    latest=latest,
                    limit=PAGINATION_LIMIT,
                ):
                    messages = conversation_history.get("messages", [])
                    if messages:
                        yield self._messages_to_file_data(messages, channel)
                        yield from self._message_files_to_file_data(messages, channel)
            except UnstructuredIngestError:
                raise
            except Exception as error:
                self._raise_for_channel_membership(error, channel, is_private=is_private)
                raise SourceConnectionError(
                    SourceConnectionError.error_string.format(str(error))
                ) from error

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
            # Auto-join public channels (and detect private ones) before reading so the
            # precheck does not fail for public channels the bot has not joined yet.
            is_private = self._prepare_channel_access(client, channel)
            # NOTE: Querying conversations history guarantees that the bot is in the channel
            try:
                client.conversations_history(channel=channel, limit=1)
            except Exception as error:
                self._raise_for_channel_membership(error, channel, is_private=is_private)
                raise

    def _prepare_channel_access(self, client: "WebClient", channel: str) -> bool:
        """Ensure the bot can read ``channel`` before indexing it.

        Public channels are auto-joined because the bot may not be a member yet. Private
        channels cannot be auto-joined; their membership errors are surfaced later (when the
        channel is read) by :meth:`_raise_for_channel_membership`.

        Returns whether the channel is private so the caller can phrase membership errors.
        """
        from slack_sdk.errors import SlackApiError

        is_private = False
        try:
            info = client.conversations_info(channel=channel)
            channel_info = info.get("channel") or {}
            is_private = bool(channel_info.get("is_private", False))
        except SlackApiError as error:
            # We could not inspect the channel (e.g. a private channel the bot is not in).
            # Defer to the read below, which raises a clear per-channel membership error.
            logger.warning(f"Unable to fetch Slack channel info for {channel}: {error}")
            return is_private

        if not is_private:
            try:
                client.conversations_join(channel=channel)
            except SlackApiError as error:
                # Best-effort: a failed join still lets the read surface the real error
                # (e.g. missing channels:join scope or an archived channel).
                logger.warning(f"Unable to auto-join public Slack channel {channel}: {error}")
        return is_private

    def _raise_for_channel_membership(
        self, error: BaseException, channel: str, *, is_private: bool
    ) -> None:
        """Translate a Slack membership error into a clear, per-channel user-facing error.

        No-op for errors that are not membership related so the caller can fall back to
        generic error handling.
        """
        code = _slack_api_error_code(error)
        if code not in CHANNEL_MEMBERSHIP_ERROR_CODES:
            return
        if is_private or code == "channel_not_found":
            raise SourceConnectionError(
                f"Bot must be invited to private channel {channel} before it can be read."
            ) from error
        raise SourceConnectionError(
            f"Bot is not a member of public channel {channel} and could not join it "
            "automatically; ensure the Slack app has the channels:join scope."
        ) from error


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
        download_url = _validate_private_download_url(download_url)

        token = self.connection_config.access_config.get_secret_value().token
        request = urllib.request.Request(
            download_url,
            headers={"Authorization": f"Bearer {token}"},
        )
        download_path.parent.mkdir(exist_ok=True, parents=True)
        await asyncio.to_thread(self._download_private_file, request, download_path)

    @staticmethod
    def _download_private_file(request: urllib.request.Request, download_path: Path) -> None:
        opener = urllib.request.build_opener(_NoRedirectHandler)
        with opener.open(
            request,
            timeout=PRIVATE_FILE_DOWNLOAD_TIMEOUT_SECONDS,
        ) as response, download_path.open("wb") as output_file:
            shutil.copyfileobj(response, output_file)

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
