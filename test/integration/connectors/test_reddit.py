"""Integration tests for the Reddit connector."""

import json
import os
from pathlib import Path
from typing import Generator

import pytest
from pytest import FixtureRequest, TempPathFactory

from unstructured_ingest.v2.interfaces import FileData
from unstructured_ingest.v2.pipeline.steps import (
    ConnectorIngestConfig,
    DownloadStep,
    IndexStep,
    StepConfig,
)
from unstructured_ingest.v2.processes.connectors.reddit import (
    RedditAccessConfig,
    RedditConnectionConfig,
    RedditIndexerConfig,
    reddit_source_entry,
)


@pytest.fixture(scope="module")
def reddit_connector_config(request: FixtureRequest) -> RedditConnectionConfig:
    """Create a Reddit connector config from environment variables."""
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT", "unstructured-ingest-test")

    if not all([client_id, client_secret]):
        pytest.skip(
            "Reddit credentials not found in environment. Need REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET"
        )

    return RedditConnectionConfig(
        access_config=RedditAccessConfig(client_secret=client_secret),
        client_id=client_id,
        user_agent=user_agent,
    )


@pytest.fixture(scope="module")
def reddit_indexer_config() -> RedditIndexerConfig:
    """Create a Reddit indexer config for testing."""
    return RedditIndexerConfig(
        subreddit_name="opensource",  # Using r/opensource as it's public and active
        num_posts=2,  # Limit to 2 posts for testing
    )


@pytest.fixture(scope="module")
def reddit_ingest_config(
    reddit_connector_config: RedditConnectionConfig,
    reddit_indexer_config: RedditIndexerConfig,
    tmp_path_factory: TempPathFactory,
) -> ConnectorIngestConfig:
    """Create an ingest config for testing."""
    download_dir = tmp_path_factory.mktemp("reddit_downloads")
    return ConnectorIngestConfig(
        connection_config=reddit_connector_config,
        indexer_config=reddit_indexer_config,
        download_dir=str(download_dir),
    )


@pytest.fixture(scope="module")
def index_step(reddit_ingest_config: ConnectorIngestConfig) -> IndexStep:
    """Create an index step for testing."""
    return IndexStep(
        source_entry=reddit_source_entry,
        config=StepConfig(ingest_config=reddit_ingest_config),
    )


@pytest.fixture(scope="module")
def download_step(reddit_ingest_config: ConnectorIngestConfig) -> DownloadStep:
    """Create a download step for testing."""
    return DownloadStep(
        source_entry=reddit_source_entry,
        config=StepConfig(ingest_config=reddit_ingest_config),
    )


def save_file_data(file_data: FileData, base_path: Path) -> None:
    """Save FileData to JSON for comparison."""
    out_path = base_path / f"{file_data.identifier}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(file_data.model_dump(), f, indent=2, sort_keys=True)


@pytest.mark.integration
def test_reddit_index_step(
    index_step: IndexStep,
    reddit_ingest_config: ConnectorIngestConfig,
) -> None:
    """Test that we can index Reddit posts."""
    file_data_list = list(index_step.run())
    
    # We should get exactly 2 posts as configured
    assert len(file_data_list) == 2
    
    # Save file data for future comparison
    test_dir = Path(__file__).parent
    expected_dir = (
        test_dir.parent / "expected_results" / "reddit" / "file_data"
    )
    expected_dir.mkdir(parents=True, exist_ok=True)

    for file_data in file_data_list:
        # Basic validation
        assert file_data.connector_type == "reddit"
        assert file_data.identifier
        assert file_data.source_identifiers.filename.endswith(".md")
        assert "opensource" in file_data.source_identifiers.fullpath
        assert file_data.metadata.url
        assert file_data.metadata.record_locator["id"]
        assert file_data.additional_metadata["subreddit"] == "opensource"
        assert file_data.additional_metadata["author"]
        assert file_data.additional_metadata["title"]

        # Save for future comparison
        save_file_data(file_data, expected_dir)


@pytest.mark.integration
def test_reddit_download_step(
    index_step: IndexStep,
    download_step: DownloadStep,
    reddit_ingest_config: ConnectorIngestConfig,
) -> None:
    """Test that we can download Reddit posts."""
    # First get the file data from indexing
    file_data_list = list(index_step.run())
    assert len(file_data_list) == 2

    # Then try to download each post
    test_dir = Path(__file__).parent
    expected_dir = (
        test_dir.parent / "expected_results" / "reddit" / "downloads"
    )
    expected_dir.mkdir(parents=True, exist_ok=True)

    for file_data in file_data_list:
        response = download_step.process_file(file_data)
        assert response.success
        assert response.download_path.exists()
        assert response.download_path.stat().st_size > 0

        # The file should contain markdown with a title and content
        content = response.download_path.read_text()
        assert content.startswith("# ")  # Should start with a title
        assert len(content.splitlines()) > 1  # Should have content after title

        # Save downloaded content for future comparison
        out_path = expected_dir / f"{file_data.identifier}.md"
        with open(out_path, "w") as f:
            f.write(content)