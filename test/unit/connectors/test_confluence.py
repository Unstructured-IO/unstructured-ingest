from unittest import mock

import pytest

from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.confluence import (
    ConfluenceAccessConfig,
    ConfluenceConnectionConfig,
    ConfluenceIndexer,
    ConfluenceIndexerConfig,
)


@pytest.fixture
def connection_config():
    """Provides a minimal ConfluenceConnectionConfig for testing."""
    access_config = ConfluenceAccessConfig(api_token="token")
    return ConfluenceConnectionConfig(
        url="https://dummy",
        username="user",
        access_config=access_config,
    )


def test_connection_config_multiple_auth():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(
                password="password",
                token="access_token",
            ),
            username="user_email",
            url="url",
        )


def test_connection_config_multiple_auth2():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(
                api_token="api_token",
                token="access_token",
            ),
            username="user_email",
            url="url",
        )


def test_connection_config_multiple_auth3():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(
                api_token="api_token",
                password="password",
            ),
            username="user_email",
            url="url",
        )


def test_connection_config_no_auth():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(access_config=ConfluenceAccessConfig(), url="url")


def test_connection_config_password_auth():
    ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(password="password"),
        url="url",
        username="user_email",
    )


def test_connection_config_api_token_auth():
    ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(api_token="api_token"),
        url="url",
        username="user_email",
    )


def test_connection_config_pat_auth():
    ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(token="access_token"),
        url="url",
    )


def test_connection_config_oauth_auth_requires_cloud_id():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(oauth_token="oauth_token"),
            url="https://example.atlassian.net/wiki",
            cloud=True,
        )


def test_connection_config_oauth_auth():
    config = ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(
            oauth_token="oauth_token",
            refresh_token="refresh_token",
        ),
        cloud_id="cloud-123",
        url="https://example.atlassian.net/wiki",
        cloud=True,
    )

    assert config.api_url() == "https://api.atlassian.com/ex/confluence/cloud-123/wiki"
    assert config.page_url("456") == "https://example.atlassian.net/wiki/pages/456"


def test_connection_config_oauth_auth_is_exclusive():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(
                api_token="api_token",
                oauth_token="oauth_token",
            ),
            cloud_id="cloud-123",
            username="user_email",
            url="https://example.atlassian.net/wiki",
            cloud=True,
        )


def test_indexer_oauth_file_data_uses_cloud_identity():
    config = ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(oauth_token="oauth_token"),
        cloud_id="cloud-123",
        url="https://example.atlassian.net/wiki",
        cloud=True,
    )
    indexer = ConfluenceIndexer(
        connection_config=config,
        index_config=ConfluenceIndexerConfig(spaces=["ENG"]),
    )
    mock_client = mock.MagicMock()
    mock_client.get_space.return_value = {"id": 987}
    mock_client.get_all_pages_from_space.return_value = [{"id": "456"}]

    with mock.patch.object(type(config), "get_client", mock.MagicMock()):
        type(config).get_client.return_value.__enter__.return_value = mock_client

        file_data = next(indexer.run())

    assert file_data.identifier == "456"
    assert file_data.source_identifiers.fullpath == "cloud-123/ENG/456.html"
    assert file_data.metadata.url == "https://example.atlassian.net/wiki/pages/456"
    assert file_data.metadata.record_locator["cloud_id"] == "cloud-123"
    assert file_data.additional_metadata["site_url"] == "https://example.atlassian.net/wiki"


def test_precheck_with_spaces_calls_get_space(monkeypatch, connection_config):
    """Test that precheck calls get_space for each space when spaces are set."""
    spaces = ["A", "B", "C"]
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=100,
        max_num_of_docs_from_each_space=100,
        spaces=spaces,
    )
    indexer = ConfluenceIndexer(connection_config=connection_config, index_config=index_config)
    mock_client = mock.MagicMock()
    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client

        result = indexer.precheck()
        calls = [mock.call(space) for space in spaces]
        mock_client.get_space.assert_has_calls(calls, any_order=False)
        assert mock_client.get_space.call_count == len(spaces)
        assert result is True


def test_precheck_without_spaces_calls_get_all_spaces(monkeypatch, connection_config):
    """Test that precheck calls get_all_spaces when spaces is not set."""
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=100,
        max_num_of_docs_from_each_space=100,
        spaces=None,
    )
    indexer = ConfluenceIndexer(connection_config=connection_config, index_config=index_config)
    mock_client = mock.MagicMock()
    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client

        result = indexer.precheck()
        mock_client.get_all_spaces.assert_called_once_with(limit=1)
        mock_client.get_space.assert_not_called()
        assert result is True


def test_precheck_with_spaces_raises(monkeypatch, connection_config):
    """Test that precheck raises UserError if get_space fails."""
    spaces = ["A", "B"]
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=100,
        max_num_of_docs_from_each_space=100,
        spaces=spaces,
    )
    indexer = ConfluenceIndexer(connection_config=connection_config, index_config=index_config)
    mock_client = mock.MagicMock()
    mock_client.get_space.side_effect = Exception("fail")
    from unstructured_ingest.processes.connectors.confluence import UserError

    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client

        with pytest.raises(UserError):
            indexer.precheck()


def test_precheck_without_spaces_raises(monkeypatch, connection_config):
    """Test that precheck raises SourceConnectionError if get_all_spaces fails."""
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=100,
        max_num_of_docs_from_each_space=100,
        spaces=None,
    )
    indexer = ConfluenceIndexer(connection_config=connection_config, index_config=index_config)
    mock_client = mock.MagicMock()
    mock_client.get_all_spaces.side_effect = Exception("fail")
    from unstructured_ingest.processes.connectors.confluence import UserError

    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client

        with pytest.raises(UserError):
            indexer.precheck()
