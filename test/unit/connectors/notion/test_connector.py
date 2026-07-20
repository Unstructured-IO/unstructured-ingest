from unittest.mock import MagicMock, patch

import pytest
from pydantic import Secret

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


def test_downloader_closes_the_client_connection_pool(connection_config):
    downloader = NotionDownloader(
        connection_config=connection_config,
        download_config=NotionDownloaderConfig(),
    )
    file_data = MagicMock()
    file_data.metadata.record_locator = {"page_id": "page-id"}

    clients = []
    build_client = NotionConnectionConfig.get_client

    def record_client(self):
        client = build_client(self)
        clients.append(client)
        return client

    with (
        patch.object(NotionConnectionConfig, "get_client", record_client),
        patch.object(NotionDownloader, "download_page", return_value="response"),
    ):
        assert downloader.run(file_data=file_data) == "response"

    assert len(clients) == 1
    assert clients[0].client.is_closed
