import asyncio
import builtins
import hashlib
import re
import shutil
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Literal, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError, ValueError, safe_error_summary
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
# The SDK excludes the initial request from max_retry_count, so 9 means 10 total attempts.
# Its 429 handlers honor Slack's Retry-After header and add 0–1 seconds of jitter.
SLACK_RATE_LIMIT_MAX_RETRIES = 9


def _slack_sync_retry_handlers():
    from slack_sdk.http_retry import ConnectionErrorRetryHandler, RateLimitErrorRetryHandler

    return [
        ConnectionErrorRetryHandler(),
        RateLimitErrorRetryHandler(max_retry_count=SLACK_RATE_LIMIT_MAX_RETRIES),
    ]


def _slack_async_retry_handlers():
    from slack_sdk.http_retry.builtin_async_handlers import (
        AsyncConnectionErrorRetryHandler,
        AsyncRateLimitErrorRetryHandler,
    )

    return [
        AsyncConnectionErrorRetryHandler(),
        AsyncRateLimitErrorRetryHandler(max_retry_count=SLACK_RATE_LIMIT_MAX_RETRIES),
    ]


def _token_kind(token: str) -> Literal["user", "bot"]:
    """Returns 'user' for xoxp- tokens, 'bot' for all others."""
    return "user" if token.startswith("xoxp-") else "bot"


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


@dataclass
class _ChannelIssue:
    channel: str
    error_code: str


def _channel_join_error_msg(error_code: str, channels: list, granted_scopes: set) -> str:
    channel_list = ", ".join(channels)
    are = "are" if len(channels) > 1 else "is"
    if error_code == "channel_not_found":
        if "channels:join" in granted_scopes:
            return (
                f"{channel_list}: not found or not accessible. "
                "Private channels cannot be auto-joined — invite the bot directly."
            )
        return (
            f"{channel_list}: not found or not accessible. "
            "Possible causes: wrong channel ID, private channel (must invite bot manually), "
            "or missing 'channels:join' scope to auto-join public channels."
        )
    if error_code == "is_archived":
        return (
            f"{channel_list} {are} archived. "
            "Slack disables all app access on archival — archived channels cannot be indexed. "
            "Unarchive the channel to index it."
        )
    if error_code == "method_not_supported_for_channel_type":
        return (
            f"{channel_list}: private channel — the bot must be invited directly before indexing. "
            "Open the channel in Slack and use /invite @<bot-name>."
        )
    if error_code == "missing_scope":
        scope_note = f" (granted: {', '.join(sorted(granted_scopes))})" if granted_scopes else ""
        return (
            f"Bot token is missing the 'channels:join' scope{scope_note}. "
            f"Cannot auto-join {channel_list}. "
            "Add the scope and reinstall the app, or invite the bot to each channel manually."
        )
    if error_code == "method_not_applicable":
        return f"{channel_list} {are} not joinable (e.g. DMs or IMs)."
    if error_code == "no_permission":
        return (
            f"Bot does not have permission to join {channel_list}. "
            "Workspace restrictions may block app channel access."
        )
    return f"Failed to join {channel_list}: {error_code}."


def _channel_history_error_msg(error_code: str, channels: list, granted_scopes: set) -> str:
    channel_list = ", ".join(channels)
    are = "are" if len(channels) > 1 else "is"
    if error_code == "not_in_channel":
        return (
            f"{channel_list}: user is not a member of this private channel(s). "
            "Ask a channel admin to invite the user."
        )
    if error_code == "channel_not_found":
        return (
            f"{channel_list}: channel not found or not accessible with this user token. "
            "Verify the channel ID is correct."
        )
    if error_code == "is_archived":
        return (
            f"{channel_list} {are} archived. "
            "Archived channels are readable by former members — "
            "if the user was not a member before archival, access will be denied."
        )
    if error_code == "missing_scope":
        scope_note = f" (granted: {', '.join(sorted(granted_scopes))})" if granted_scopes else ""
        return (
            f"User token is missing a required scope {scope_note}. "
            f"Re-authorize the token with channels:history for public channels and groups:history "
            f"for private channels to read {channel_list}."
        )
    if error_code in ("not_authed", "invalid_auth", "token_revoked"):
        return (
            f"Authentication failed for {channel_list}: {error_code}. "
            "Check that the user token is valid and has not expired or been revoked."
        )
    return f"Cannot read history for {channel_list}: {error_code}."


class SlackAccessConfig(AccessConfig):
    token: str = Field(
        description="Bot token (xoxb-…) or user token (xoxp-…) for the Slack API. "
        "Both require channels:history scope. "
        "With a bot token the connector attempts to auto-join public channels; "
        "with a user token it does not — user tokens can view public channels without joining."
    )
    refresh_token: Optional[str] = Field(default=None, description="Slack OAuth refresh token.")


class SlackConnectionConfig(ConnectionConfig):
    access_config: Secret[SlackAccessConfig]

    @requires_dependencies(["slack_sdk"], extras="slack")
    @SourceConnectionError.wrap
    def get_client(self) -> "WebClient":
        from slack_sdk import WebClient

        return WebClient(
            token=self.access_config.get_secret_value().token,
            retry_handlers=_slack_sync_retry_handlers(),
        )

    @requires_dependencies(["slack_sdk"], extras="slack")
    @SourceConnectionError.wrap
    def get_async_client(self) -> "AsyncWebClient":
        from slack_sdk.web.async_client import AsyncWebClient

        return AsyncWebClient(
            token=self.access_config.get_secret_value().token,
            retry_handlers=_slack_async_retry_handlers(),
        )


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
        granted_scopes = self._get_granted_scopes(client)
        token = self.connection_config.access_config.get_secret_value().token
        self._validate_channels(client, _token_kind(token), granted_scopes)
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
        for channel in self.index_config.channels:
            # NOTE: Iterate ALL conversations.history pages so a channel is grouped into stable
            # per-UTC-day packages regardless of the SDK's internal pagination.
            messages: list[dict] = []
            for conversation_history in client.conversations_history(
                channel=channel,
                oldest=oldest,
                latest=latest,
                limit=PAGINATION_LIMIT,
            ):
                messages.extend(conversation_history.get("messages", []))

            if not messages:
                continue

            for day, day_messages in self._group_messages_by_day(messages).items():
                yield self._messages_to_file_data(day_messages, channel, client, day)

            for file_data in self._message_files_to_file_data(messages, channel):
                yield file_data

    @staticmethod
    def _message_day(message: dict) -> Optional[str]:
        # NOTE: Top-level messages are grouped by their own ts-day; thread replies belong to
        # their ROOT (thread_ts) day. conversations.history only returns parents/standalone
        # messages, but using thread_ts when present keeps replies pinned to the root's day.
        day_ts = message.get("thread_ts") or message.get("ts")
        if not day_ts:
            return None
        return datetime.fromtimestamp(float(day_ts), tz=timezone.utc).strftime("%Y-%m-%d")

    def _group_messages_by_day(self, messages: list[dict]) -> dict[str, list[dict]]:
        packages: dict[str, list[dict]] = defaultdict(list)
        for message in messages:
            day = self._message_day(message)
            if day is None:
                continue
            packages[day].append(message)
        return {day: packages[day] for day in sorted(packages)}

    @staticmethod
    def _package_version(messages: list[dict]) -> Optional[str]:
        # NOTE: The newest activity in the package. Includes parent latest_reply and edited.ts
        # (both returned by conversations.history on the parent) so an old day whose thread gets
        # a new reply or whose message is edited bumps its version and updates in place.
        candidates: list[str] = []
        for message in messages:
            for value in (
                message.get("ts"),
                message.get("latest_reply"),
                (message.get("edited") or {}).get("ts"),
            ):
                if value:
                    candidates.append(value)
        if not candidates:
            return None
        return max(candidates, key=float)

    def _messages_to_file_data(
        self,
        messages: list[dict],
        channel: str,
        client: Optional["WebClient"] = None,
        day: str = "",
    ) -> FileData:
        timestamps = [message["ts"] for message in messages if message.get("ts")]
        ts_oldest = min(timestamps, key=float)
        ts_latest = max(timestamps, key=float)
        version = self._package_version(messages)

        # NOTE: Stable across reruns so a modified day UPDATES in place instead of duplicating.
        identifier_base = f"{channel}-{day}"
        identifier = hashlib.sha256(identifier_base.encode("utf-8")).hexdigest()
        filename = identifier[:16]

        permalink = None
        if client is not None:
            try:
                response = client.chat_getPermalink(channel=channel, message_ts=ts_oldest)
                raw = response.get("permalink")
                permalink = raw if isinstance(raw, str) else None
            except Exception:
                logger.debug(f"Could not retrieve permalink for channel={channel} ts={ts_oldest}.")

        source_identifiers = SourceIdentifiers(
            filename=f"{filename}.xml", fullpath=f"{filename}.xml"
        )
        return FileData(
            identifier=identifier,
            connector_type=CONNECTOR_TYPE,
            source_identifiers=source_identifiers,
            metadata=FileDataSourceMetadata(
                url=permalink,
                version=version,
                date_created=ts_oldest,
                date_modified=version,
                date_processed=str(time.time()),
                record_locator={
                    "channel": channel,
                    "day": day,
                    "oldest": ts_oldest,
                    "latest": ts_latest,
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
                        url=slack_file.get("permalink") or None,
                        version=message_ts,
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

    def _get_granted_scopes(self, client: "WebClient") -> set:
        try:
            response = client.auth_test()
            # x-oauth-scopes is an HTTP response header present on every Slack API response.
            # The SDK stores raw HTTP headers in response.headers as a plain dict whose keys
            # preserve server casing, so use case-insensitive lookup.
            scopes_header = next(
                (v for k, v in response.headers.items() if k.lower() == "x-oauth-scopes"),
                "",
            )
            return {s.strip() for s in scopes_header.split(",") if s.strip()}
        except Exception:
            return set()

    def _validate_channels_bot(self, client: "WebClient", granted_scopes: set) -> None:
        from slack_sdk.errors import SlackApiError

        issues: list[_ChannelIssue] = []
        for channel in self.index_config.channels:
            try:
                client.conversations_join(channel=channel)
            except SlackApiError as e:
                error_code = e.response.get("error", "unknown")
                if error_code in ("missing_scope", "method_not_supported_for_channel_type"):
                    # Can't auto-join (missing scope or private channel); check if already a member.
                    try:
                        client.conversations_history(channel=channel, limit=1)
                        continue
                    except SlackApiError:
                        pass
                issues.append(_ChannelIssue(channel=channel, error_code=error_code))

        if issues:
            groups: dict[str, list] = defaultdict(list)
            for issue in issues:
                groups[issue.error_code].append(issue.channel)
            lines = [
                _channel_join_error_msg(error_code, channels, granted_scopes)
                for error_code, channels in groups.items()
            ]
            raise SourceConnectionError(
                f"Cannot access {len(issues)} channel(s):\n"
                + "\n".join(f"  - {line}" for line in lines)
            )

    def _validate_channels_user(self, client: "WebClient", granted_scopes: set) -> None:
        from slack_sdk.errors import SlackApiError

        issues: list[_ChannelIssue] = []
        for channel in self.index_config.channels:
            try:
                client.conversations_history(channel=channel, limit=1)
            except SlackApiError as e:
                error_code = e.response.get("error", "unknown")
                issues.append(_ChannelIssue(channel=channel, error_code=error_code))

        if issues:
            groups: dict[str, list] = defaultdict(list)
            for issue in issues:
                groups[issue.error_code].append(issue.channel)
            lines = [
                _channel_history_error_msg(error_code, channels, granted_scopes)
                for error_code, channels in groups.items()
            ]
            raise SourceConnectionError(
                f"Cannot access {len(issues)} channel(s) with user token:\n"
                + "\n".join(f"  - {line}" for line in lines)
            )

    def _validate_channels(
        self, client: "WebClient", token_kind: Literal["user", "bot"], granted_scopes: set
    ) -> None:
        if token_kind == "user":
            self._validate_channels_user(client, granted_scopes)
        else:
            self._validate_channels_bot(client, granted_scopes)

    def precheck(self) -> None:
        try:
            client = self.connection_config.get_client()
            granted_scopes = self._get_granted_scopes(client)
            token = self.connection_config.access_config.get_secret_value().token
            self._validate_channels(client, _token_kind(token), granted_scopes)
        except SourceConnectionError:
            raise
        except Exception as e:
            # 'from None' suppresses the provider exception chain so its text
            # cannot resurface via traceback logging.
            raise SourceConnectionError(
                f"failed to validate connection: {safe_error_summary(e)}"
            ) from None


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
            # NOTE: oldest/latest bound the indexed message range. The indexer stores the
            # oldest and newest top-level message timestamps; inclusive=True keeps both ends so
            # the downloader fetches the exact same set of thread roots the indexer grouped.
            oldest=file_data.metadata.record_locator["oldest"],
            latest=file_data.metadata.record_locator["latest"],
            limit=PAGINATION_LIMIT,
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
        with (
            opener.open(
                request,
                timeout=PRIVATE_FILE_DOWNLOAD_TIMEOUT_SECONDS,
            ) as response,
            download_path.open("wb") as output_file,
        ):
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
