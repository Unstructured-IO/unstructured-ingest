from unittest.mock import MagicMock, patch

import pytest
from pydantic import Secret
from pytest_mock import MockerFixture

from unstructured_ingest.processes.connectors.notion.connector import (
    NotionAccessConfig,
    NotionConnectionConfig,
    NotionDownloader,
    NotionDownloaderConfig,
)


@pytest.fixture()
def connection_config() -> NotionConnectionConfig:
    return NotionConnectionConfig(
        access_config=Secret(NotionAccessConfig(notion_api_key="fake-key")),
    )


def test_downloader_closes_the_client_connection_pool(connection_config, mocker: MockerFixture):
    downloader = NotionDownloader(
        connection_config=connection_config,
        download_config=NotionDownloaderConfig(),
    )
    file_data = MagicMock()
    file_data.metadata.record_locator = {"page_id": "page-id"}

    get_client = mocker.spy(NotionConnectionConfig, "get_client")

    with patch.object(NotionDownloader, "download_page", return_value="response"):
        assert downloader.run(file_data=file_data) == "response"

    get_client.assert_called_once()
    assert get_client.spy_return.client.is_closed
