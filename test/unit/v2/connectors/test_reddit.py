"""Unit tests for the Reddit v2 connector."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.v2.processes.connectors.reddit import (
    RedditAccessConfig,
    RedditConnectionConfig,
    RedditIndexer,
    RedditIndexerConfig,
    RedditDownloader,
    reddit_source_entry,
)


def test_access_config_requires_client_secret():
    """Test that RedditAccessConfig requires client_secret."""
    with pytest.raises(ValidationError):
        RedditAccessConfig()


def test_connection_config_validates():
    """Test that RedditConnectionConfig validates correctly."""
    config = RedditConnectionConfig(
        access_config=RedditAccessConfig(client_secret="secret123"),
        client_id="client123",
        user_agent="test_agent",
    )
    assert config.client_id == "client123"
    assert config.user_agent == "test_agent"
    assert config.access_config.get_secret_value().client_secret == "secret123"


def test_indexer_config_validates_num_posts():
    """Test that RedditIndexerConfig validates num_posts."""
    with pytest.raises(ValidationError):
        RedditIndexerConfig(
            subreddit_name="test",
            num_posts=0,
        )

    config = RedditIndexerConfig(
        subreddit_name="test",
        num_posts=10,
    )
    assert config.num_posts == 10
    assert config.subreddit_name == "test"
    assert config.search_query is None


@patch("praw.Reddit")
def test_reddit_client_creation(mock_reddit):
    """Test Reddit client creation."""
    config = RedditConnectionConfig(
        access_config=RedditAccessConfig(client_secret="secret123"),
        client_id="client123",
        user_agent="test_agent",
    )
    
    client = config.get_client()
    mock_reddit.assert_called_once_with(
        client_id="client123",
        client_secret="secret123",
        user_agent="test_agent",
    )


@patch("praw.Reddit")
def test_indexer_precheck_success(mock_reddit):
    """Test successful precheck."""
    mock_subreddit = MagicMock()
    mock_subreddit.hot.return_value = iter([MagicMock()])
    mock_reddit.return_value.subreddit.return_value = mock_subreddit

    connection_config = RedditConnectionConfig(
        access_config=RedditAccessConfig(client_secret="secret123"),
        client_id="client123",
        user_agent="test_agent",
    )
    index_config = RedditIndexerConfig(
        subreddit_name="test",
        num_posts=10,
    )
    
    indexer = RedditIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    
    indexer.precheck()  # Should not raise
    mock_reddit.return_value.subreddit.assert_called_once_with("test")


@patch("praw.Reddit")
def test_indexer_precheck_failure(mock_reddit):
    """Test failed precheck."""
    mock_reddit.return_value.subreddit.side_effect = Exception("API Error")

    connection_config = RedditConnectionConfig(
        access_config=RedditAccessConfig(client_secret="secret123"),
        client_id="client123",
        user_agent="test_agent",
    )
    index_config = RedditIndexerConfig(
        subreddit_name="test",
        num_posts=10,
    )
    
    indexer = RedditIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@patch("praw.Reddit")
def test_indexer_run_hot_posts(mock_reddit):
    """Test indexing hot posts."""
    mock_post = MagicMock(
        id="123",
        created_utc=1612137600,  # Example timestamp
        permalink="/r/test/comments/123/test_post",
        subreddit=MagicMock(display_name="test"),
        author="test_user",
        title="Test Post",
    )
    mock_subreddit = MagicMock()
    mock_subreddit.hot.return_value = [mock_post]
    mock_reddit.return_value.subreddit.return_value = mock_subreddit

    connection_config = RedditConnectionConfig(
        access_config=RedditAccessConfig(client_secret="secret123"),
        client_id="client123",
        user_agent="test_agent",
    )
    index_config = RedditIndexerConfig(
        subreddit_name="test",
        num_posts=1,
    )
    
    indexer = RedditIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    
    file_data_list = list(indexer.run())
    assert len(file_data_list) == 1
    file_data = file_data_list[0]
    
    assert file_data.identifier == "123"
    assert file_data.connector_type == "reddit"
    assert file_data.source_identifiers.filename == "123.md"
    assert file_data.source_identifiers.fullpath == "test/123.md"
    assert file_data.metadata.url == "/r/test/comments/123/test_post"
    assert file_data.additional_metadata["subreddit"] == "test"
    assert file_data.additional_metadata["author"] == "test_user"
    assert file_data.additional_metadata["title"] == "Test Post"


@patch("praw.Reddit")
def test_downloader_run(mock_reddit):
    """Test downloading a post."""
    mock_post = MagicMock(
        id="123",
        title="Test Post",
        selftext="Test Content",
        author="test_user",
    )
    mock_submission = MagicMock()
    mock_submission.return_value = mock_post
    mock_reddit.return_value.submission = mock_submission

    connection_config = RedditConnectionConfig(
        access_config=RedditAccessConfig(client_secret="secret123"),
        client_id="client123",
        user_agent="test_agent",
    )
    
    downloader = RedditDownloader(connection_config=connection_config)
    
    with patch("pathlib.Path.mkdir"), patch("builtins.open", create=True):
        response = downloader.run(
            file_data=MagicMock(
                identifier="123",
                metadata=MagicMock(record_locator={"id": "123"}),
            )
        )
        assert response.success is True


def test_source_registry_entry():
    """Test that the source registry entry is properly configured."""
    assert reddit_source_entry.indexer == RedditIndexer
    assert reddit_source_entry.indexer_config == RedditIndexerConfig
    assert reddit_source_entry.downloader == RedditDownloader
    assert reddit_source_entry.connection_config == RedditConnectionConfig