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
    file_data = mocker.MagicMock()
    file_data.metadata.record_locator = {"page_id": "page-id"}

    clients = []
    mocker.patch.object(
        NotionDownloader,
        "download_page",
        side_effect=lambda client, page_id, file_data: clients.append(client) or "response",
    )

    assert downloader.run(file_data=file_data) == "response"

    assert len(clients) == 1
    assert clients[0].client.is_closed
