from unittest.mock import Mock

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.processes.connectors.sharepoint import (
    SharepointConnectionConfig,
    SharepointDownloader,
    SharepointDownloaderConfig,
)


@pytest.fixture
def mock_client():
    return Mock()


@pytest.fixture
def mock_site():
    return Mock()


@pytest.fixture
def mock_drive_item():
    return Mock()


@pytest.fixture
def mock_file():
    return Mock()


@pytest.fixture
def mock_connection_config(mock_client, mock_drive_item):
    config = Mock(spec=SharepointConnectionConfig)
    config.site = "https://test.sharepoint.com/sites/test"
    config.get_client.return_value = mock_client
    config._get_drive_item.return_value = mock_drive_item
    return config


@pytest.fixture
def mock_download_config():
    config = Mock(spec=SharepointDownloaderConfig)
    config.max_retries = 3
    return config


@pytest.fixture
def sharepoint_downloader(mock_connection_config, mock_download_config):
    downloader = SharepointDownloader(
        connection_config=mock_connection_config, download_config=mock_download_config
    )
    return downloader


@pytest.fixture
def file_data():
    return FileData(
        source_identifiers=SourceIdentifiers(
            filename="test.docx", fullpath="/sites/test/Shared Documents/test.docx"
        ),
        connector_type="sharepoint",
        identifier="test-id",
    )


def test_fetch_file_retries_on_429_error(
    mock_client, mock_drive_item, mock_site, sharepoint_downloader, file_data
):
    """Test that _fetch_file retries when encountering 429 errors"""
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.return_value = (
        mock_site
    )
    mock_drive_item.get_by_path.return_value.get.return_value.execute_query.side_effect = [
        Exception("429 Client Error"),
        Exception("Request has been throttled"),
        mock_file,
    ]

    result = sharepoint_downloader._fetch_file(file_data)
    assert result == mock_file
    assert mock_drive_item.get_by_path.return_value.get.return_value.execute_query.call_count == 3


def test_fetch_file_fails_after_max_retries(
    mock_client, mock_drive_item, mock_site, sharepoint_downloader, file_data
):
    """Test that _fetch_file fails after exhausting max retries"""
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.return_value = (
        mock_site
    )
    mock_drive_item.get_by_path.return_value.get.return_value.execute_query.side_effect = Exception(
        "429 Client Error"
    )

    with pytest.raises(Exception, match="429"):
        sharepoint_downloader._fetch_file(file_data)

    expected_calls = sharepoint_downloader.download_config.max_retries
    assert (
        mock_drive_item.get_by_path.return_value.get.return_value.execute_query.call_count
        == expected_calls
    )


def test_fetch_file_handles_site_not_found_immediately(
    mock_client, sharepoint_downloader, file_data
):
    """Test that site not found errors are not retried"""
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        Exception("Site not found")
    )

    with pytest.raises(SourceConnectionError, match="Site not found"):
        sharepoint_downloader._fetch_file(file_data)

    assert mock_client.sites.get_by_url.return_value.get.return_value.execute_query.call_count == 1


def test_fetch_file_without_errors(
    mock_client, mock_drive_item, mock_site, mock_file, sharepoint_downloader, file_data
):
    """Test successful file fetch without any errors"""
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.return_value = (
        mock_site
    )
    mock_drive_item.get_by_path.return_value.get.return_value.execute_query.return_value = mock_file
    result = sharepoint_downloader._fetch_file(file_data)

    assert result == mock_file
    assert mock_client.sites.get_by_url.return_value.get.return_value.execute_query.call_count == 1
    assert mock_drive_item.get_by_path.return_value.get.return_value.execute_query.call_count == 1
    mock_drive_item.get_by_path.assert_called_with("/sites/test/Shared Documents/test.docx")
