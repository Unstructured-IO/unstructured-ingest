from unittest.mock import Mock

import pytest

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.processes.connectors.slack import (
    SlackDownloader,
    SlackIndexer,
    SlackIndexerConfig,
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
        source_identifiers=SourceIdentifiers(filename="F123-report.pdf", fullpath="F123-report.pdf"),
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
    response.read.return_value = b"pdf bytes"
    urlopen = mocker.patch("urllib.request.urlopen", return_value=response)

    await downloader._download_file(file_data, tmp_path / "F123-report.pdf")

    assert (tmp_path / "F123-report.pdf").read_bytes() == b"pdf bytes"
    urlopen.assert_called_once()
    request = urlopen.call_args.args[0]
    assert request.full_url == "https://files.slack.com/report.pdf"
    assert request.headers["Authorization"] == "Bearer xoxb-slack-token"
