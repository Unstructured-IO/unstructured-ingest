import hashlib
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from slack_sdk.errors import SlackApiError

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.slack import (
    PRIVATE_FILE_DOWNLOAD_TIMEOUT_SECONDS,
    SlackAccessConfig,
    SlackDownloader,
    SlackIndexer,
    SlackIndexerConfig,
    _NoRedirectHandler,
)

CHANNEL = "C123"
DAY_1 = datetime(2024, 3, 9, tzinfo=timezone.utc)
DAY_2 = datetime(2024, 3, 10, tzinfo=timezone.utc)


def _ts(day: datetime, *, hours: int = 0, minutes: int = 0, micro: int = 0) -> str:
    """Build a Slack timestamp string on a specific UTC day."""
    seconds = int(day.timestamp()) + hours * 3600 + minutes * 60
    return f"{seconds}.{micro:06d}"


def _day_str(day: datetime) -> str:
    return day.strftime("%Y-%m-%d")


def _expected_conversation_identifier(channel: str, day: datetime) -> str:
    return hashlib.sha256(f"{channel}-{_day_str(day)}".encode("utf-8")).hexdigest()


def _run_indexer(messages: list[dict], channels: list[str] | None = None) -> list[FileData]:
    client = Mock()
    client.conversations_history.return_value = [{"messages": messages}]
    # Public channel that auto-joins cleanly, with a string permalink so metadata.url is valid.
    client.conversations_info.return_value = {"channel": {"is_private": False}}
    client.conversations_join.return_value = {"ok": True}
    client.chat_getPermalink.return_value = {
        "ok": True,
        "permalink": "https://slack.test/archives/C123/p1",
    }
    connection_config = Mock()
    connection_config.get_client.return_value = client
    indexer = SlackIndexer(
        index_config=SlackIndexerConfig(channels=channels or [CHANNEL]),
        connection_config=connection_config,
    )
    return list(indexer.run())


def _conversations(file_data: list[FileData]) -> list[FileData]:
    return [fd for fd in file_data if fd.source_identifiers.filename.endswith(".xml")]


def _slack_api_error(code: str) -> SlackApiError:
    return SlackApiError(message=code, response={"ok": False, "error": code})


def _make_indexer(client, channel: str) -> SlackIndexer:
    # Provide a string permalink by default so metadata.url validates for tests that build a
    # conversation package. Tests asserting permalink behavior override this explicitly.
    if not isinstance(client.chat_getPermalink.return_value, dict):
        client.chat_getPermalink.return_value = {
            "ok": True,
            "permalink": "https://slack.test/archives/C123/p1",
        }
    connection_config = Mock()
    connection_config.get_client.return_value = client
    return SlackIndexer(
        index_config=SlackIndexerConfig(channels=[channel]),
        connection_config=connection_config,
    )


def test_slack_access_config_accepts_refresh_token():
    config = SlackAccessConfig(token="xoxb-slack-token", refresh_token="xoxe-slack-refresh-token")

    assert config.token == "xoxb-slack-token"
    assert config.refresh_token == "xoxe-slack-refresh-token"


def test_slack_indexer_emits_file_data_for_message_files():
    conversation_permalink = "https://acme.slack.com/archives/C123/p1710000000000100"
    file_permalink = "https://acme.slack.com/files/U123/F123/report.pdf"
    client = Mock()
    client.conversations_history.return_value = [
        {
            "messages": [
                {
                    "ts": "1710000000.000100",
                    "text": "Here is the report",
                    "files": [
                        {
                            "id": "F123",
                            "name": "report.pdf",
                            "url_private_download": "https://files.slack.com/report.pdf",
                            "permalink": file_permalink,
                        }
                    ],
                }
            ]
        }
    ]
    client.chat_getPermalink.return_value = {"ok": True, "permalink": conversation_permalink}
    connection_config = Mock()
    connection_config.get_client.return_value = client
    indexer = SlackIndexer(
        index_config=SlackIndexerConfig(channels=["C123"]),
        connection_config=connection_config,
    )

    file_data = list(indexer.run())

    assert len(file_data) == 2
    conversation = file_data[0]
    assert conversation.source_identifiers.filename.endswith(".xml")
    assert conversation.metadata.url == conversation_permalink
    client.chat_getPermalink.assert_called_once_with(channel="C123", message_ts="1710000000.000100")
    slack_file = file_data[1]
    assert slack_file.source_identifiers.filename == "F123-report.pdf"
    assert slack_file.metadata.url == file_permalink
    assert slack_file.metadata.record_locator == {
        "type": "file",
        "channel": "C123",
        "message_ts": "1710000000.000100",
        "file_id": "F123",
    }


def test_slack_conversation_url_uses_oldest_message_permalink():
    oldest_permalink = "https://acme.slack.com/archives/C123/p1710000000000100"
    client = Mock()
    client.conversations_history.return_value = [
        {
            "messages": [
                {"ts": "1710000200.000300", "text": "newest"},
                {"ts": "1710000000.000100", "text": "oldest"},
            ]
        }
    ]
    client.chat_getPermalink.return_value = {"ok": True, "permalink": oldest_permalink}
    connection_config = Mock()
    connection_config.get_client.return_value = client
    indexer = SlackIndexer(
        index_config=SlackIndexerConfig(channels=["C123"]),
        connection_config=connection_config,
    )

    file_data = list(indexer.run())

    assert file_data[0].metadata.url == oldest_permalink
    client.chat_getPermalink.assert_called_once_with(channel="C123", message_ts="1710000000.000100")


def test_slack_conversation_url_fails_soft_to_none():
    client = Mock()
    client.conversations_history.return_value = [
        {"messages": [{"ts": "1710000000.000100", "text": "hello"}]}
    ]
    client.chat_getPermalink.side_effect = Exception("ratelimited")
    connection_config = Mock()
    connection_config.get_client.return_value = client
    indexer = SlackIndexer(
        index_config=SlackIndexerConfig(channels=["C123"]),
        connection_config=connection_config,
    )

    file_data = list(indexer.run())

    assert file_data[0].metadata.url is None


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


def test_slack_indexer_auto_joins_public_channel_before_read():
    client = Mock()
    client.conversations_info.return_value = {"channel": {"is_private": False}}
    client.conversations_join.return_value = {"ok": True}
    client.conversations_history.return_value = [
        {"messages": [{"ts": "1710000000.000100", "text": "hello"}]}
    ]
    indexer = _make_indexer(client, "C-PUBLIC")

    file_data = list(indexer.run())

    # Public, non-member channels are auto-joined before being read.
    client.conversations_join.assert_called_once_with(channel="C-PUBLIC")
    assert len(file_data) == 1
    assert file_data[0].source_identifiers.filename.endswith(".xml")


def test_slack_indexer_private_channel_not_in_channel_raises_user_error():
    client = Mock()
    client.conversations_info.return_value = {"channel": {"is_private": True}}
    client.conversations_history.side_effect = _slack_api_error("not_in_channel")
    indexer = _make_indexer(client, "C-PRIVATE")

    with pytest.raises(SourceConnectionError, match="C-PRIVATE") as exc_info:
        list(indexer.run())

    # The error must name the channel and tell the user to invite the bot.
    assert "invited" in str(exc_info.value).lower()
    # Private channels cannot be auto-joined.
    client.conversations_join.assert_not_called()


def test_slack_indexer_private_channel_not_found_raises_user_error():
    client = Mock()
    # conversations.info itself fails for a private channel the bot is not in.
    client.conversations_info.side_effect = _slack_api_error("channel_not_found")
    client.conversations_history.side_effect = _slack_api_error("channel_not_found")
    indexer = _make_indexer(client, "C-MISSING")

    with pytest.raises(SourceConnectionError, match="C-MISSING"):
        list(indexer.run())

    client.conversations_join.assert_not_called()


def test_slack_precheck_auto_joins_public_channel():
    client = Mock()
    client.conversations_info.return_value = {"channel": {"is_private": False}}
    client.conversations_join.return_value = {"ok": True}
    client.conversations_history.return_value = {"messages": []}
    indexer = _make_indexer(client, "C-PUBLIC")

    indexer.precheck()

    client.conversations_join.assert_called_once_with(channel="C-PUBLIC")
    client.conversations_history.assert_called_once_with(channel="C-PUBLIC", limit=1)


def test_slack_precheck_private_not_in_channel_raises_user_error():
    client = Mock()
    client.conversations_info.return_value = {"channel": {"is_private": True}}
    client.conversations_history.side_effect = _slack_api_error("not_in_channel")
    indexer = _make_indexer(client, "C-PRIVATE")

    with pytest.raises(SourceConnectionError, match="C-PRIVATE") as exc_info:
        indexer.precheck()

    assert "invited" in str(exc_info.value).lower()
    client.conversations_join.assert_not_called()


def test_slack_indexer_join_failure_is_best_effort():
    client = Mock()
    client.conversations_info.return_value = {"channel": {"is_private": False}}
    # Missing channels:join scope should not crash; the read still succeeds for a member bot.
    client.conversations_join.side_effect = _slack_api_error("missing_scope")
    client.conversations_history.return_value = [
        {"messages": [{"ts": "1710000000.000100", "text": "hello"}]}
    ]
    indexer = _make_indexer(client, "C-PUBLIC")

    file_data = list(indexer.run())

    client.conversations_join.assert_called_once_with(channel="C-PUBLIC")
    assert len(file_data) == 1


def test_slack_indexer_groups_messages_into_one_package_per_day():
    """All messages in the same UTC day collapse into a single stable conversation package."""
    messages = [
        {"ts": _ts(DAY_1, hours=1)},
        {"ts": _ts(DAY_1, hours=5)},
    ]

    conversations = _conversations(_run_indexer(messages))

    assert len(conversations) == 1
    conversation = conversations[0]
    assert conversation.identifier == _expected_conversation_identifier(CHANNEL, DAY_1)
    assert conversation.metadata.record_locator == {
        "channel": CHANNEL,
        "day": _day_str(DAY_1),
        "oldest": _ts(DAY_1, hours=1),
        "latest": _ts(DAY_1, hours=5),
    }
    # version/date_modified track the newest activity; date_created is the oldest message.
    assert conversation.metadata.version == _ts(DAY_1, hours=5)
    assert conversation.metadata.date_modified == _ts(DAY_1, hours=5)
    assert conversation.metadata.date_created == _ts(DAY_1, hours=1)


def test_slack_same_day_new_top_level_message_keeps_identifier_and_bumps_version():
    """A new top-level message on the same day keeps the identifier but bumps the version."""
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
    # Version bumped to the newest message.
    assert original.metadata.version == _ts(DAY_1, hours=1)
    assert updated.metadata.version == _ts(DAY_1, hours=9)


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


def test_slack_file_data_sets_version_to_message_ts():
    """Files keep their stable identifier and set version to the message ts."""
    message_ts = _ts(DAY_1, hours=4)
    file_data = _run_indexer(
        [
            {
                "ts": message_ts,
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

    slack_files = [
        fd for fd in file_data if fd.identifier != _expected_conversation_identifier(CHANNEL, DAY_1)
    ]
    assert len(slack_files) == 1
    slack_file = slack_files[0]
    expected_identifier = hashlib.sha256(f"{CHANNEL}-{message_ts}-F123".encode("utf-8")).hexdigest()
    assert slack_file.identifier == expected_identifier
    assert slack_file.metadata.version == message_ts
