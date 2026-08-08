import hashlib
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import Mock, call
from urllib.error import HTTPError

import pytest
from pydantic import Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.slack import (
    PRIVATE_FILE_DOWNLOAD_TIMEOUT_SECONDS,
    SLACK_RATE_LIMIT_MAX_RETRIES,
    SlackAccessConfig,
    SlackConnectionConfig,
    SlackDownloader,
    SlackIndexer,
    SlackIndexerConfig,
    _channel_history_error_msg,
    _channel_join_error_msg,
    _NoRedirectHandler,
    _slack_async_retry_handlers,
    _slack_sync_retry_handlers,
    _token_kind,
)

CHANNEL = "C123"
DAY_1 = datetime(2024, 3, 9, tzinfo=timezone.utc)
DAY_2 = datetime(2024, 3, 10, tzinfo=timezone.utc)
BOT_TOKEN = "xoxb-bot"
USER_TOKEN = "xoxp-user"


def test_token_kind_user_prefix():
    assert _token_kind("xoxp-12345") == "user"
    assert _token_kind("xoxe.xoxp-12345") == "user", "Refreshed variant incorrectly classified"


def test_token_kind_bot_prefix():
    assert _token_kind("xoxb-12345") == "bot"
    assert _token_kind("xoxe.xoxb-12345") == "bot", "Refreshed variant incorrectly classified"


def test_token_kind_unknown_prefix_is_bot():
    assert _token_kind("xoxa-12345") == "bot"
    assert _token_kind("xoxe.xoxa-12345") == "bot"


def _ts(day: datetime, *, hours: int = 0, minutes: int = 0, micro: int = 0) -> str:
    """Build a Slack timestamp string on a specific UTC day."""
    seconds = int(day.timestamp()) + hours * 3600 + minutes * 60
    return f"{seconds}.{micro:06d}"


def _day_str(day: datetime) -> str:
    return day.strftime("%Y-%m-%d")


def _expected_conversation_identifier(channel: str, day: datetime) -> str:
    return hashlib.sha256(f"{channel}-{_day_str(day)}".encode("utf-8")).hexdigest()


def _run_indexer(
    messages: list[dict],
    channels: list[str] | None = None,
    *,
    pages: list[list[dict]] | None = None,
) -> list[FileData]:
    client = Mock()
    if pages is not None:
        client.conversations_history.return_value = [{"messages": page} for page in pages]
    else:
        client.conversations_history.return_value = [{"messages": messages}]
    client.conversations_join.return_value = {"ok": True}
    client.auth_test.return_value = Mock(headers={})
    client.chat_getPermalink.return_value = {
        "ok": True,
        "permalink": "https://slack.test/archives/C123/p1",
    }
    connection_config = Mock()
    connection_config.get_client.return_value = client
    connection_config.access_config.get_secret_value.return_value.token = BOT_TOKEN
    indexer = SlackIndexer(
        index_config=SlackIndexerConfig(channels=channels or [CHANNEL]),
        connection_config=connection_config,
    )
    return list(indexer.run())


def _conversations(file_data: list[FileData]) -> list[FileData]:
    return [fd for fd in file_data if fd.source_identifiers.filename.endswith(".xml")]


def test_slack_access_config_accepts_refresh_token():
    config = SlackAccessConfig(token="xoxb-slack-token", refresh_token="xoxe-slack-refresh-token")

    assert config.token == "xoxb-slack-token"
    assert config.refresh_token == "xoxe-slack-refresh-token"


def _make_indexer(channels=None):
    return SlackIndexer(
        index_config=SlackIndexerConfig(channels=channels or ["C123"]),
        connection_config=Mock(),
    )


def test_slack_indexer_emits_file_data_for_message_files():
    message_ts = "1710000000.000100"
    file_data = _run_indexer(
        [
            {
                "ts": message_ts,
                "text": "Here is the report",
                "files": [
                    {
                        "id": "F123",
                        "name": "report.pdf",
                        "url_private_download": "https://files.slack.com/report.pdf",
                    }
                ],
            }
        ]
    )

    assert len(file_data) == 2
    assert file_data[0].source_identifiers.filename.endswith(".xml")
    slack_file = file_data[1]
    expected_identifier = hashlib.sha256(f"{CHANNEL}-{message_ts}-F123".encode("utf-8")).hexdigest()
    assert slack_file.identifier == expected_identifier
    assert slack_file.source_identifiers.filename == "F123-report.pdf"
    assert slack_file.metadata.version == message_ts
    assert slack_file.metadata.record_locator == {
        "type": "file",
        "channel": CHANNEL,
        "message_ts": message_ts,
        "file_id": "F123",
    }


def test_messages_to_file_data_includes_permalink():
    client = Mock()
    client.chat_getPermalink.return_value.get.return_value = (
        "https://my-workspace.slack.com/archives/C123/p1710000000000100"
    )
    indexer = _make_indexer()

    file_data = indexer._messages_to_file_data(
        messages=[{"ts": "1710000000.000100", "text": "hello"}],
        channel="C123",
        client=client,
        day="2024-03-09",
    )

    assert file_data.metadata.url == (
        "https://my-workspace.slack.com/archives/C123/p1710000000000100"
    )
    client.chat_getPermalink.assert_called_once_with(channel="C123", message_ts="1710000000.000100")


def test_messages_to_file_data_uses_oldest_ts_for_permalink():
    client = Mock()
    client.chat_getPermalink.return_value.get.return_value = "https://example.slack.com/p"
    indexer = _make_indexer()

    indexer._messages_to_file_data(
        messages=[
            {"ts": "1710000002.000000", "text": "newer"},
            {"ts": "1710000001.000000", "text": "older"},
        ],
        channel="C123",
        client=client,
        day="2024-03-09",
    )

    client.chat_getPermalink.assert_called_once_with(channel="C123", message_ts="1710000001.000000")


def test_messages_to_file_data_omits_url_without_client():
    file_data = _make_indexer()._messages_to_file_data(
        messages=[{"ts": "1710000000.000100", "text": "hello"}],
        channel="C123",
        day="2024-03-09",
    )

    assert file_data.metadata.url is None


def test_messages_to_file_data_omits_url_when_permalink_fetch_fails():
    client = Mock()
    client.chat_getPermalink.side_effect = Exception("API error")
    indexer = _make_indexer()

    file_data = indexer._messages_to_file_data(
        messages=[{"ts": "1710000000.000100", "text": "hello"}],
        channel="C123",
        client=client,
        day="2024-03-09",
    )

    assert file_data.metadata.url is None


def test_message_files_to_file_data_includes_permalink():
    messages = [
        {
            "ts": "1710000000.000100",
            "files": [
                {
                    "id": "F123",
                    "name": "report.pdf",
                    "permalink": "https://my-workspace.slack.com/files/U123/F123/report.pdf",
                }
            ],
        }
    ]

    file_data_list = list(_make_indexer()._message_files_to_file_data(messages, "C123"))

    assert len(file_data_list) == 1
    assert file_data_list[0].metadata.url == (
        "https://my-workspace.slack.com/files/U123/F123/report.pdf"
    )


def test_message_files_to_file_data_omits_url_when_no_permalink():
    messages = [
        {
            "ts": "1710000000.000100",
            "files": [{"id": "F123", "name": "report.pdf"}],
        }
    ]

    file_data_list = list(_make_indexer()._message_files_to_file_data(messages, "C123"))

    assert len(file_data_list) == 1
    assert file_data_list[0].metadata.url is None


@pytest.mark.asyncio
async def test_slack_downloader_downloads_file_attachment(tmp_path, mocker):
    async_client = Mock()
    async_client.files_info = mocker.AsyncMock(
        return_value={
            "ok": True,
            "file": {
                "id": "F123",
                "name": "report.pdf",
                "url_private_download": "https://files.slack.com/report.pdf",
            },
        }
    )
    connection_config = Mock()
    connection_config.get_async_client.return_value = async_client
    connection_config.access_config.get_secret_value.return_value.token = "xoxb-slack-token"
    downloader = SlackDownloader(connection_config=connection_config)
    file_data = FileData(
        identifier="F123",
        connector_type="slack",
        source_identifiers=SourceIdentifiers(
            filename="F123-report.pdf",
            fullpath="F123-report.pdf",
        ),
        metadata=FileDataSourceMetadata(
            record_locator={
                "type": "file",
                "channel": "C123",
                "message_ts": "1710000000.000100",
                "file_id": "F123",
            }
        ),
    )
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=None)
    response.read.side_effect = [b"pdf ", b"bytes", b""]
    opener = Mock()
    opener.open.return_value = response
    build_opener = mocker.patch("urllib.request.build_opener", return_value=opener)

    await downloader._download_file(file_data, tmp_path / "F123-report.pdf")

    assert (tmp_path / "F123-report.pdf").read_bytes() == b"pdf bytes"
    build_opener.assert_called_once_with(_NoRedirectHandler)
    opener.open.assert_called_once()
    request = opener.open.call_args.args[0]
    assert request.full_url == "https://files.slack.com/report.pdf"
    assert request.headers["Authorization"] == "Bearer xoxb-slack-token"
    assert opener.open.call_args.kwargs["timeout"] == PRIVATE_FILE_DOWNLOAD_TIMEOUT_SECONDS
    assert response.read.call_count > 1


def test_slack_private_file_download_rejects_redirects():
    handler = _NoRedirectHandler()

    with pytest.raises(IngestValueError, match="redirected"):
        handler.redirect_request(None, None, 302, "Found", {}, "https://example.com/report.pdf")


@pytest.mark.asyncio
async def test_slack_downloader_rejects_non_slack_private_download_url(tmp_path, mocker):
    async_client = Mock()
    async_client.files_info = mocker.AsyncMock(
        return_value={
            "ok": True,
            "file": {
                "id": "F123",
                "name": "report.pdf",
                "url_private_download": "https://example.com/report.pdf",
            },
        }
    )
    connection_config = Mock()
    connection_config.get_async_client.return_value = async_client
    connection_config.access_config.get_secret_value.return_value.token = "xoxb-slack-token"
    downloader = SlackDownloader(connection_config=connection_config)
    file_data = FileData(
        identifier="F123",
        connector_type="slack",
        source_identifiers=SourceIdentifiers(
            filename="F123-report.pdf",
            fullpath="F123-report.pdf",
        ),
        metadata=FileDataSourceMetadata(
            record_locator={
                "type": "file",
                "channel": "C123",
                "message_ts": "1710000000.000100",
                "file_id": "F123",
            }
        ),
    )
    build_opener = mocker.patch("urllib.request.build_opener")

    with pytest.raises(IngestValueError, match="files.slack.com"):
        await downloader._download_file(file_data, tmp_path / "F123-report.pdf")

    build_opener.assert_not_called()


# ---------------------------------------------------------------------------
# Auto-join / channel validation tests
# ---------------------------------------------------------------------------


def _make_slack_api_error(error_code: str):
    from slack_sdk.errors import SlackApiError

    response = Mock()
    response.get.return_value = error_code
    return SlackApiError(message=error_code, response=response)


def _make_rate_limit_http_error() -> HTTPError:
    from email.message import Message

    headers = Message()
    headers.add_header("Retry-After", "3")
    return HTTPError(
        url="https://slack.com/api/conversations.join",
        code=429,
        msg="Too Many Requests",
        hdrs=headers,
        fp=BytesIO(b'{"ok":false,"error":"rate_limited"}'),
    )


def _raise_rate_limit_http_error(*_args, **_kwargs) -> HTTPError:
    raise _make_rate_limit_http_error()


def test_slack_sync_retry_handlers_include_connection_and_rate_limit_handlers():
    from slack_sdk.http_retry import ConnectionErrorRetryHandler, RateLimitErrorRetryHandler

    handlers = _slack_sync_retry_handlers()

    assert len(handlers) == 2
    assert isinstance(handlers[0], ConnectionErrorRetryHandler)
    assert isinstance(handlers[1], RateLimitErrorRetryHandler)
    assert handlers[1].max_retry_count == SLACK_RATE_LIMIT_MAX_RETRIES


def test_slack_async_retry_handlers_include_connection_and_rate_limit_handlers():
    from slack_sdk.http_retry.builtin_async_handlers import (
        AsyncConnectionErrorRetryHandler,
        AsyncRateLimitErrorRetryHandler,
    )

    handlers = _slack_async_retry_handlers()

    assert len(handlers) == 2
    assert isinstance(handlers[0], AsyncConnectionErrorRetryHandler)
    assert isinstance(handlers[1], AsyncRateLimitErrorRetryHandler)
    assert handlers[1].max_retry_count == SLACK_RATE_LIMIT_MAX_RETRIES


def test_slack_connection_config_get_client_configures_retry_handlers():
    from slack_sdk.http_retry import ConnectionErrorRetryHandler, RateLimitErrorRetryHandler

    config = SlackConnectionConfig(access_config=Secret(SlackAccessConfig(token=BOT_TOKEN)))
    client = config.get_client()

    assert len(client.retry_handlers) == 2
    assert isinstance(client.retry_handlers[0], ConnectionErrorRetryHandler)
    assert isinstance(client.retry_handlers[1], RateLimitErrorRetryHandler)
    assert client.retry_handlers[1].max_retry_count == SLACK_RATE_LIMIT_MAX_RETRIES


def test_slack_connection_config_get_async_client_configures_retry_handlers():
    from slack_sdk.http_retry.builtin_async_handlers import (
        AsyncConnectionErrorRetryHandler,
        AsyncRateLimitErrorRetryHandler,
    )

    config = SlackConnectionConfig(access_config=Secret(SlackAccessConfig(token=BOT_TOKEN)))
    client = config.get_async_client()

    assert len(client.retry_handlers) == 2
    assert isinstance(client.retry_handlers[0], AsyncConnectionErrorRetryHandler)
    assert isinstance(client.retry_handlers[1], AsyncRateLimitErrorRetryHandler)
    assert client.retry_handlers[1].max_retry_count == SLACK_RATE_LIMIT_MAX_RETRIES


def test_slack_sync_client_honors_retry_after_with_jitter_until_retries_exhausted(mocker):
    from slack_sdk.errors import SlackApiError

    sleep = mocker.patch("slack_sdk.http_retry.builtin_handlers.time.sleep")
    mocker.patch("slack_sdk.http_retry.builtin_handlers.random.random", return_value=0.25)
    mocker.patch(
        "slack_sdk.web.base_client.BaseClient._perform_urllib_http_request_internal",
        side_effect=_raise_rate_limit_http_error,
    )

    config = SlackConnectionConfig(access_config=Secret(SlackAccessConfig(token=BOT_TOKEN)))
    client = config.get_client()

    with pytest.raises(SlackApiError):
        client.conversations_join(channel="C1")

    assert sleep.call_args_list == [call(3.25)] * SLACK_RATE_LIMIT_MAX_RETRIES


@pytest.mark.asyncio
async def test_slack_async_client_honors_retry_after_with_jitter_until_retries_exhausted(mocker):
    from slack_sdk.errors import SlackApiError

    sleep = mocker.patch("slack_sdk.http_retry.builtin_async_handlers.asyncio.sleep")
    mocker.patch("slack_sdk.http_retry.builtin_async_handlers.random.random", return_value=0.25)

    class MockResponse:
        status = 429
        headers = {"Retry-After": "3"}
        content_type = "application/json"

        async def json(self):
            return {"ok": False, "error": "rate_limited"}

        async def text(self):
            return '{"ok": false, "error": "rate_limited"}'

        async def read(self):
            return b'{"ok": false, "error": "rate_limited"}'

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    session = mocker.Mock()
    session.closed = False
    session.request = mocker.Mock(return_value=MockResponse())
    session.close = mocker.AsyncMock()

    config = SlackConnectionConfig(access_config=Secret(SlackAccessConfig(token=BOT_TOKEN)))
    client = config.get_async_client()
    client.session = session
    connection_config = mocker.Mock()
    connection_config.get_async_client.return_value = client
    downloader = SlackDownloader(connection_config=connection_config)
    file_data = FileData(
        identifier="pkg-1",
        connector_type="slack",
        source_identifiers=SourceIdentifiers(filename="abc.xml", fullpath="abc.xml"),
        metadata=FileDataSourceMetadata(
            record_locator={
                "channel": CHANNEL,
                "oldest": "1.0",
                "latest": "2.0",
            }
        ),
    )

    with pytest.raises(SlackApiError):
        await downloader._download_conversation(file_data, Mock())

    assert sleep.call_args_list == [call(3.25)] * SLACK_RATE_LIMIT_MAX_RETRIES


def test_validate_channels_bot_succeeds_when_all_joins_succeed():
    client = Mock()
    indexer = _make_indexer(channels=["C1", "C2"])

    indexer._validate_channels_bot(client, granted_scopes=set())

    assert client.conversations_join.call_count == 2


def test_validate_channels_bot_raises_for_failed_channel():
    client = Mock()
    client.conversations_join.side_effect = _make_slack_api_error("channel_not_found")
    indexer = _make_indexer(channels=["C1"])

    with pytest.raises(SourceConnectionError, match="C1"):
        indexer._validate_channels_bot(client, granted_scopes=set())


def test_validate_channels_bot_groups_same_error_across_channels():
    client = Mock()
    client.conversations_join.side_effect = _make_slack_api_error("is_archived")
    indexer = _make_indexer(channels=["C1", "C2"])

    with pytest.raises(SourceConnectionError) as exc_info:
        indexer._validate_channels_bot(client, granted_scopes=set())

    msg = str(exc_info.value)
    assert "C1, C2" in msg
    # Both channels grouped onto one bullet, not one line per channel
    assert msg.count("  - ") == 1


def test_validate_channels_bot_reports_all_failures_together():
    def join_side_effect(channel, **_):
        if channel == "C1":
            raise _make_slack_api_error("channel_not_found")
        if channel == "C2":
            raise _make_slack_api_error("is_archived")

    client = Mock()
    client.conversations_join.side_effect = join_side_effect
    indexer = _make_indexer(channels=["C1", "C2"])

    with pytest.raises(SourceConnectionError) as exc_info:
        indexer._validate_channels_bot(client, granted_scopes=set())

    msg = str(exc_info.value)
    assert "C1" in msg
    assert "C2" in msg
    assert "2 channel" in msg


def test_validate_channels_bot_missing_scope_succeeds_if_already_member():
    client = Mock()
    client.conversations_join.side_effect = _make_slack_api_error("missing_scope")
    indexer = _make_indexer(channels=["C1"])

    indexer._validate_channels_bot(client, granted_scopes=set())

    client.conversations_history.assert_called_once_with(channel="C1", limit=1)


def test_validate_channels_bot_missing_scope_fails_if_not_member():

    def history_side_effect(**_):
        raise _make_slack_api_error("not_in_channel")

    client = Mock()
    client.conversations_join.side_effect = _make_slack_api_error("missing_scope")
    client.conversations_history.side_effect = history_side_effect
    indexer = _make_indexer(channels=["C1"])

    with pytest.raises(SourceConnectionError, match="channels:join"):
        indexer._validate_channels_bot(client, granted_scopes={"channels:history"})


def test_validate_channels_bot_archived_always_fails():
    client = Mock()
    client.conversations_join.side_effect = _make_slack_api_error("is_archived")
    indexer = _make_indexer(channels=["C1"])

    with pytest.raises(SourceConnectionError, match="archived"):
        indexer._validate_channels_bot(client, granted_scopes=set())

    client.conversations_history.assert_not_called()


def test_validate_channels_bot_private_succeeds_if_already_invited():
    client = Mock()
    client.conversations_join.side_effect = _make_slack_api_error(
        "method_not_supported_for_channel_type"
    )
    indexer = _make_indexer(channels=["C1"])

    indexer._validate_channels_bot(client, granted_scopes=set())

    client.conversations_history.assert_called_once_with(channel="C1", limit=1)


def test_validate_channels_bot_private_fails_if_not_invited():
    client = Mock()
    client.conversations_join.side_effect = _make_slack_api_error(
        "method_not_supported_for_channel_type"
    )
    client.conversations_history.side_effect = _make_slack_api_error("not_in_channel")
    indexer = _make_indexer(channels=["C1"])

    with pytest.raises(SourceConnectionError, match="private"):
        indexer._validate_channels_bot(client, granted_scopes=set())


def test_validate_channels_user_does_not_call_join():
    client = Mock()
    indexer = _make_indexer(channels=["C1", "C2"])

    indexer._validate_channels_user(client, granted_scopes=set())

    client.conversations_join.assert_not_called()
    assert client.conversations_history.call_count == 2


def test_validate_channels_user_raises_when_history_fails():
    client = Mock()
    client.conversations_history.side_effect = _make_slack_api_error("not_in_channel")
    indexer = _make_indexer(channels=["C1"])

    with pytest.raises(SourceConnectionError, match="user token"):
        indexer._validate_channels_user(client, granted_scopes=set())


def test_validate_channels_user_succeeds_when_history_accessible():
    client = Mock()
    client.conversations_history.return_value = [{"messages": []}]
    indexer = _make_indexer(channels=["C1"])

    indexer._validate_channels_user(client, granted_scopes=set())

    client.conversations_history.assert_called_once_with(channel="C1", limit=1)


def test_validate_channels_user_groups_errors():
    client = Mock()
    client.conversations_history.side_effect = _make_slack_api_error("is_archived")
    indexer = _make_indexer(channels=["C1", "C2"])

    with pytest.raises(SourceConnectionError) as exc_info:
        indexer._validate_channels_user(client, granted_scopes=set())

    msg = str(exc_info.value)
    assert "C1, C2" in msg
    assert msg.count("  - ") == 1


def test_validate_channels_dispatches_bot_for_bot_token():
    client = Mock()
    indexer = _make_indexer(channels=["C1"])

    indexer._validate_channels(client, token_kind="bot", granted_scopes=set())

    client.conversations_join.assert_called_once_with(channel="C1")


def test_validate_channels_dispatches_user_for_user_token():
    client = Mock()
    indexer = _make_indexer(channels=["C1"])

    indexer._validate_channels(client, token_kind="user", granted_scopes=set())

    client.conversations_join.assert_not_called()
    client.conversations_history.assert_called_once_with(channel="C1", limit=1)


def test_channel_join_error_msg_private_channel_hint_when_join_scope_present():
    msg = _channel_join_error_msg("channel_not_found", ["C1"], granted_scopes={"channels:join"})
    assert "private" in msg.lower()
    assert "invite" in msg.lower()


def test_channel_join_error_msg_groups_multiple_archived_channels():
    msg = _channel_join_error_msg("is_archived", ["C1", "C2"], granted_scopes=set())
    assert "C1, C2" in msg
    assert "are archived" in msg


def test_channel_join_error_msg_private_channel_not_invited():
    msg = _channel_join_error_msg(
        "method_not_supported_for_channel_type", ["C1"], granted_scopes=set()
    )
    assert "private" in msg.lower()
    assert "invite" in msg.lower()


def test_run_joins_channels_before_yielding():
    client = Mock()
    client.conversations_history.return_value = []
    client.conversations_join.side_effect = _make_slack_api_error("channel_not_found")
    connection_config = Mock()
    connection_config.get_client.return_value = client
    connection_config.access_config.get_secret_value.return_value.token = BOT_TOKEN
    indexer = SlackIndexer(
        index_config=SlackIndexerConfig(channels=["C1"]),
        connection_config=connection_config,
    )

    with pytest.raises(SourceConnectionError):
        list(indexer.run())

    client.conversations_join.assert_called_once_with(channel="C1")


def test_run_does_not_join_channels_with_user_token():
    client = Mock()
    client.conversations_history.return_value = []
    connection_config = Mock()
    connection_config.get_client.return_value = client
    connection_config.access_config.get_secret_value.return_value.token = USER_TOKEN
    indexer = SlackIndexer(
        index_config=SlackIndexerConfig(channels=["C1"]),
        connection_config=connection_config,
    )

    list(indexer.run())

    client.conversations_join.assert_not_called()


def test_precheck_uses_channel_validation():
    client = Mock()
    client.conversations_join.side_effect = _make_slack_api_error("is_archived")
    connection_config = Mock()
    connection_config.get_client.return_value = client
    connection_config.access_config.get_secret_value.return_value.token = BOT_TOKEN
    indexer = SlackIndexer(
        index_config=SlackIndexerConfig(channels=["C1"]),
        connection_config=connection_config,
    )

    with pytest.raises(SourceConnectionError, match="archived"):
        indexer.precheck()


def test_precheck_user_token_does_not_join():
    client = Mock()
    client.conversations_history.return_value = [{"messages": []}]
    connection_config = Mock()
    connection_config.get_client.return_value = client
    connection_config.access_config.get_secret_value.return_value.token = USER_TOKEN
    indexer = SlackIndexer(
        index_config=SlackIndexerConfig(channels=["C1"]),
        connection_config=connection_config,
    )

    indexer.precheck()

    client.conversations_join.assert_not_called()


def test_slack_same_day_new_top_level_message_keeps_identifier_and_bumps_version():
    """Same-day messages collapse into one package; new messages keep id and bump version."""
    first_run = _conversations(_run_indexer([{"ts": _ts(DAY_1, hours=1)}]))
    assert len(first_run) == 1
    original = first_run[0]

    second_run = _conversations(
        _run_indexer(
            [
                {"ts": _ts(DAY_1, hours=1)},
                {"ts": _ts(DAY_1, hours=9)},
            ]
        )
    )
    assert len(second_run) == 1
    updated = second_run[0]

    # Same identifier => updates in place (no duplicate document).
    assert updated.identifier == original.identifier
    assert updated.identifier == _expected_conversation_identifier(CHANNEL, DAY_1)
    assert updated.metadata.record_locator == {
        "channel": CHANNEL,
        "day": _day_str(DAY_1),
        "oldest": _ts(DAY_1, hours=1),
        "latest": _ts(DAY_1, hours=9),
    }
    # Version/date_modified track the newest activity; date_created is the oldest message.
    assert original.metadata.version == _ts(DAY_1, hours=1)
    assert updated.metadata.version == _ts(DAY_1, hours=9)
    assert updated.metadata.date_modified == _ts(DAY_1, hours=9)
    assert updated.metadata.date_created == _ts(DAY_1, hours=1)


def test_slack_indexer_merges_paginated_history_into_one_package_per_day():
    """Messages across conversations.history pages collapse into a single day package."""
    conversations = _conversations(
        _run_indexer(
            [],
            pages=[
                [{"ts": _ts(DAY_1, hours=1)}],
                [{"ts": _ts(DAY_1, hours=5)}],
            ],
        )
    )

    assert len(conversations) == 1
    conversation = conversations[0]
    assert conversation.identifier == _expected_conversation_identifier(CHANNEL, DAY_1)
    assert conversation.metadata.record_locator == {
        "channel": CHANNEL,
        "day": _day_str(DAY_1),
        "oldest": _ts(DAY_1, hours=1),
        "latest": _ts(DAY_1, hours=5),
    }
    assert conversation.metadata.version == _ts(DAY_1, hours=5)


def test_slack_reply_to_old_day_thread_keeps_identifier_and_bumps_version():
    """A reply to an old day's thread keeps that day's identifier and bumps version via
    latest_reply, even when the reply itself lands on a later day."""
    root_ts = _ts(DAY_1, hours=2)
    before = _conversations(_run_indexer([{"ts": root_ts, "thread_ts": root_ts}]))
    assert len(before) == 1
    original = before[0]
    assert original.metadata.version == root_ts

    # A reply arrives on DAY_2 but the thread root is still on DAY_1.
    reply_ts = _ts(DAY_2, hours=3)
    after = _conversations(
        _run_indexer(
            [
                {
                    "ts": root_ts,
                    "thread_ts": root_ts,
                    "latest_reply": reply_ts,
                    "reply_count": 1,
                }
            ]
        )
    )
    assert len(after) == 1
    updated = after[0]

    # Still pinned to DAY_1's identifier (the root's day), not DAY_2.
    assert updated.identifier == original.identifier
    assert updated.identifier == _expected_conversation_identifier(CHANNEL, DAY_1)
    assert updated.metadata.record_locator["day"] == _day_str(DAY_1)
    # Version detected the reply cheaply via latest_reply on the parent.
    assert updated.metadata.version == reply_ts
    assert updated.metadata.date_modified == reply_ts


def test_slack_edited_message_bumps_version_via_edited_ts():
    """An edited message bumps the package version via edited.ts."""
    root_ts = _ts(DAY_1, hours=2)
    edited_ts = _ts(DAY_1, hours=6)
    conversations = _conversations(
        _run_indexer([{"ts": root_ts, "edited": {"user": "U1", "ts": edited_ts}}])
    )

    assert len(conversations) == 1
    assert conversations[0].metadata.version == edited_ts


def test_slack_indexer_yields_days_most_recent_first():
    """Within a channel, conversation-day packages are emitted newest day first."""
    conversations = _conversations(
        _run_indexer(
            [
                {"ts": _ts(DAY_1, hours=1)},
                {"ts": _ts(DAY_2, hours=1)},
            ]
        )
    )

    assert [c.metadata.record_locator["day"] for c in conversations] == [
        _day_str(DAY_2),
        _day_str(DAY_1),
    ]


def test_slack_indexer_interleaves_recent_file_ahead_of_older_conversation():
    """A file on a recent message sorts ahead of an older day's conversation package."""
    old_ts = _ts(DAY_1, hours=1)
    recent_ts = _ts(DAY_2, hours=1)
    records = _run_indexer(
        [
            {"ts": old_ts, "text": "old message"},
            {
                "ts": recent_ts,
                "text": "recent message with file",
                "files": [
                    {
                        "id": "F123",
                        "name": "report.pdf",
                        "url_private_download": "https://files.slack.com/report.pdf",
                    }
                ],
            },
        ]
    )

    versions = [float(fd.metadata.version) for fd in records]
    assert versions == sorted(versions, reverse=True)

    file_index = next(
        i for i, fd in enumerate(records) if fd.source_identifiers.filename == "F123-report.pdf"
    )
    old_conversation_index = next(
        i
        for i, fd in enumerate(records)
        if fd.metadata.record_locator.get("day") == _day_str(DAY_1)
    )
    assert file_index < old_conversation_index


def _file_data_with_version(version) -> FileData:
    return FileData(
        identifier="id",
        connector_type="slack",
        source_identifiers=SourceIdentifiers(filename="f.xml", fullpath="f.xml"),
        metadata=FileDataSourceMetadata(version=version),
    )


def test_recency_key_parses_version_timestamp():
    assert SlackIndexer._recency_key(_file_data_with_version("1710000000.000100")) == pytest.approx(
        1710000000.000100
    )


def test_recency_key_falls_back_to_zero_for_missing_version():
    assert SlackIndexer._recency_key(_file_data_with_version(None)) == 0.0


def test_recency_key_falls_back_to_zero_for_unparseable_version():
    assert SlackIndexer._recency_key(_file_data_with_version("not-a-timestamp")) == 0.0


def test_slack_new_day_creates_new_identifier():
    """Messages on a new UTC day create a separate package with a new identifier."""
    conversations = _conversations(
        _run_indexer(
            [
                {"ts": _ts(DAY_1, hours=1)},
                {"ts": _ts(DAY_2, hours=1)},
            ]
        )
    )

    assert len(conversations) == 2
    identifiers = {conversation.identifier for conversation in conversations}
    assert identifiers == {
        _expected_conversation_identifier(CHANNEL, DAY_1),
        _expected_conversation_identifier(CHANNEL, DAY_2),
    }


def test_message_day_from_ts():
    assert SlackIndexer._message_day({"ts": _ts(DAY_1, hours=3)}) == _day_str(DAY_1)


def test_message_day_prefers_thread_ts():
    root = _ts(DAY_1, hours=2)
    reply = _ts(DAY_2, hours=3)
    assert SlackIndexer._message_day({"ts": reply, "thread_ts": root}) == _day_str(DAY_1)


def test_message_day_returns_none_without_ts():
    assert SlackIndexer._message_day({}) is None


def test_group_messages_by_day_sorts_days_chronologically():
    groups = _make_indexer()._group_messages_by_day(
        [
            {"ts": _ts(DAY_2, hours=1)},
            {"ts": _ts(DAY_1, hours=1)},
        ]
    )

    assert list(groups.keys()) == [_day_str(DAY_1), _day_str(DAY_2)]
    assert len(groups[_day_str(DAY_1)]) == 1
    assert len(groups[_day_str(DAY_2)]) == 1


def test_package_version_picks_newest_activity():
    root = _ts(DAY_1, hours=1)
    reply = _ts(DAY_1, hours=5)
    edited = _ts(DAY_1, hours=8)
    assert (
        SlackIndexer._package_version(
            [{"ts": root, "latest_reply": reply, "edited": {"ts": edited}}]
        )
        == edited
    )


# ---------------------------------------------------------------------------
# _channel_history_error_msg (user-token path)
# ---------------------------------------------------------------------------


def test_channel_history_error_msg_not_in_channel():
    msg = _channel_history_error_msg("not_in_channel", ["C1"], granted_scopes=set())
    assert "private" in msg.lower()
    assert "invite" in msg.lower()


def test_channel_history_error_msg_channel_not_found():
    msg = _channel_history_error_msg("channel_not_found", ["C1"], granted_scopes=set())
    assert "not found" in msg.lower() or "accessible" in msg.lower()


def test_channel_history_error_msg_archived():
    msg = _channel_history_error_msg("is_archived", ["C1"], granted_scopes=set())
    assert "archived" in msg.lower()


def test_channel_history_error_msg_archived_multiple_channels():
    msg = _channel_history_error_msg("is_archived", ["C1", "C2"], granted_scopes=set())
    assert "C1, C2" in msg
    assert "are" in msg


def test_channel_history_error_msg_missing_scope():
    msg = _channel_history_error_msg("missing_scope", ["C1"], granted_scopes=set())
    assert "channels:history" in msg


def test_channel_history_error_msg_missing_scope_includes_granted():
    msg = _channel_history_error_msg("missing_scope", ["C1"], granted_scopes={"channels:read"})
    assert "channels:read" in msg


def test_channel_history_error_msg_invalid_auth():
    msg = _channel_history_error_msg("invalid_auth", ["C1"], granted_scopes=set())
    assert "token" in msg.lower() or "auth" in msg.lower()


def test_channel_history_error_msg_token_revoked():
    msg = _channel_history_error_msg("token_revoked", ["C1"], granted_scopes=set())
    assert "revoked" in msg.lower() or "token" in msg.lower()


def test_channel_history_error_msg_unknown_error():
    msg = _channel_history_error_msg("some_weird_error", ["C1"], granted_scopes=set())
    assert "some_weird_error" in msg
