from unittest.mock import Mock

import pytest

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.slack import (
    PRIVATE_FILE_DOWNLOAD_TIMEOUT_SECONDS,
    SlackAccessConfig,
    SlackDownloader,
    SlackIndexer,
    SlackIndexerConfig,
    _NoRedirectHandler,
)


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
    client.chat_getPermalink.return_value.get.return_value = None
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
    )

    assert file_data.metadata.url == (
        "https://my-workspace.slack.com/archives/C123/p1710000000000100"
    )
    client.chat_getPermalink.assert_called_once_with(
        channel="C123", message_ts="1710000000.000100"
    )


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
    )

    client.chat_getPermalink.assert_called_once_with(
        channel="C123", message_ts="1710000001.000000"
    )


def test_messages_to_file_data_omits_url_without_client():
    file_data = _make_indexer()._messages_to_file_data(
        messages=[{"ts": "1710000000.000100", "text": "hello"}],
        channel="C123",
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
