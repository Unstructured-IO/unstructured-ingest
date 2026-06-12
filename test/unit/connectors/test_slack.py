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


def _slack_api_error(code: str) -> SlackApiError:
    return SlackApiError(message=code, response={"ok": False, "error": code})


def _make_indexer(client, channel: str) -> SlackIndexer:
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
                        }
                    ],
                }
            ]
        }
    ]
    connection_config = Mock()
    connection_config.get_client.return_value = client
    indexer = SlackIndexer(
        index_config=SlackIndexerConfig(channels=["C123"]),
        connection_config=connection_config,
    )

    file_data = list(indexer.run())

    assert len(file_data) == 2
    assert file_data[0].source_identifiers.filename.endswith(".xml")
    slack_file = file_data[1]
    assert slack_file.source_identifiers.filename == "F123-report.pdf"
    assert slack_file.metadata.record_locator == {
        "type": "file",
        "channel": "C123",
        "message_ts": "1710000000.000100",
        "file_id": "F123",
    }


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
