"""Unit tests for Elasticsearch connector."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import Secret

from unstructured_ingest.data_types.file_data import BatchItem, FileDataSourceMetadata
from unstructured_ingest.processes.connectors.elasticsearch.elasticsearch import (
    ElasticsearchAccessConfig,
    ElasticsearchBatchFileData,
    ElasticsearchConnectionConfig,
    ElasticsearchDownloader,
    ElasticsearchDownloaderConfig,
    ElastisearchAdditionalMetadata,
)
from unstructured_ingest.utils.dep_check import dependency_exists

# Skip all tests in this module if elasticsearch is not available
pytestmark = pytest.mark.skipif(
    not dependency_exists("elasticsearch"),
    reason="elasticsearch is not installed. Install with: pip install 'unstructured-ingest[elasticsearch]'",
)


@pytest.fixture
def connection_config():
    """Provides a minimal ElasticsearchConnectionConfig for testing."""
    access_config = ElasticsearchAccessConfig(password="test_password")
    return ElasticsearchConnectionConfig(
        hosts=["http://localhost:9200"],
        username="test_user",
        access_config=Secret(access_config),
    )


@pytest.fixture
def mock_async_client():
    """Provides a mock AsyncElasticsearch client."""
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock()
    return client


@pytest.fixture
def mock_file_data():
    """Provides a mock BatchFileData for testing."""
    return ElasticsearchBatchFileData(
        connector_type="elasticsearch",
        metadata=FileDataSourceMetadata(
            url="http://localhost:9200/test_index",
            date_processed="123456",
        ),
        additional_metadata=ElastisearchAdditionalMetadata(index_name="test_index"),
        batch_items=[
            BatchItem(identifier="doc1"),
            BatchItem(identifier="doc2"),
            BatchItem(identifier="doc3"),
        ],
        display_name="test",
    )


async def mock_async_scan(client, query, scroll, index):
    """Mock async_scan that yields test documents."""
    test_docs = [
        {
            "_id": "doc1",
            "_version": 1,
            "_source": {"field1": "value1", "field2": "value2"},
        },
        {
            "_id": "doc2",
            "_version": 1,
            "_source": {"field1": "value3", "field2": "value4"},
        },
        {
            "_id": "doc3",
            "_version": 1,
            "_source": {"field1": "value5", "field2": "value6"},
        },
    ]
    for doc in test_docs:
        yield doc


@pytest.mark.asyncio
async def test_run_async_with_empty_fields_omits_source(
    connection_config: ElasticsearchConnectionConfig,
    mock_file_data: ElasticsearchBatchFileData,
    mock_async_client: AsyncMock,
    tmp_path: Path,
):
    """
    Test that when fields is empty, _source is NOT included in the scan_query.
    This prevents AWS OpenSearch FGAC timeout issues with empty lists.
    """
    download_config = ElasticsearchDownloaderConfig(
        download_dir=tmp_path,
        fields=[],  # Empty fields list
    )
    downloader = ElasticsearchDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    captured_query = None

    async def capture_query_async_scan(client, query, scroll, index):
        nonlocal captured_query
        captured_query = query
        async for doc in mock_async_scan(client, query, scroll, index):
            yield doc

    with (
        patch(
            "unstructured_ingest.processes.connectors.elasticsearch.elasticsearch.AsyncElasticsearch",
            return_value=mock_async_client,
        ),
        patch(
            "unstructured_ingest.processes.connectors.elasticsearch.elasticsearch.async_scan",
            side_effect=capture_query_async_scan,
        ),
    ):
        results = await downloader.run_async(file_data=mock_file_data)

        # Verify _source was NOT included in the query
        assert captured_query is not None
        assert "_source" not in captured_query
        assert "version" in captured_query
        assert captured_query["version"] is True
        assert "query" in captured_query
        assert captured_query["query"]["ids"]["values"] == ["doc1", "doc2", "doc3"]

        # Verify we got download responses
        assert len(results) == 3


@pytest.mark.asyncio
async def test_run_async_with_no_fields_omits_source(
    connection_config: ElasticsearchConnectionConfig,
    mock_file_data: ElasticsearchBatchFileData,
    mock_async_client: AsyncMock,
    tmp_path: Path,
):
    """
    Test that when fields is None/not specified, _source is NOT included in the scan_query.
    """
    download_config = ElasticsearchDownloaderConfig(
        download_dir=tmp_path,
        # fields not specified, defaults to empty list
    )
    downloader = ElasticsearchDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    captured_query = None

    async def capture_query_async_scan(client, query, scroll, index):
        nonlocal captured_query
        captured_query = query
        async for doc in mock_async_scan(client, query, scroll, index):
            yield doc

    with (
        patch(
            "unstructured_ingest.processes.connectors.elasticsearch.elasticsearch.AsyncElasticsearch",
            return_value=mock_async_client,
        ),
        patch(
            "unstructured_ingest.processes.connectors.elasticsearch.elasticsearch.async_scan",
            side_effect=capture_query_async_scan,
        ),
    ):
        results = await downloader.run_async(file_data=mock_file_data)

        # Verify _source was NOT included in the query
        assert captured_query is not None
        assert "_source" not in captured_query
        assert "version" in captured_query
        assert captured_query["version"] is True

        # Verify we got download responses
        assert len(results) == 3


@pytest.mark.asyncio
async def test_run_async_with_specified_fields_includes_source(
    connection_config: ElasticsearchConnectionConfig,
    mock_file_data: ElasticsearchBatchFileData,
    mock_async_client: AsyncMock,
    tmp_path: Path,
):
    """
    Test that when fields are explicitly specified, _source IS included in the scan_query
    with those specific fields.
    """
    test_fields = ["field1", "field2", "field3"]
    download_config = ElasticsearchDownloaderConfig(
        download_dir=tmp_path,
        fields=test_fields,
    )
    downloader = ElasticsearchDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    captured_query = None

    async def capture_query_async_scan(client, query, scroll, index):
        nonlocal captured_query
        captured_query = query
        async for doc in mock_async_scan(client, query, scroll, index):
            yield doc

    with (
        patch(
            "unstructured_ingest.processes.connectors.elasticsearch.elasticsearch.AsyncElasticsearch",
            return_value=mock_async_client,
        ),
        patch(
            "unstructured_ingest.processes.connectors.elasticsearch.elasticsearch.async_scan",
            side_effect=capture_query_async_scan,
        ),
    ):
        results = await downloader.run_async(file_data=mock_file_data)

        # Verify _source WAS included with the specified fields
        assert captured_query is not None
        assert "_source" in captured_query
        assert captured_query["_source"] == test_fields
        assert "version" in captured_query
        assert captured_query["version"] is True

        # Verify we got download responses
        assert len(results) == 3


@pytest.mark.asyncio
async def test_run_async_with_single_field_includes_source(
    connection_config: ElasticsearchConnectionConfig,
    mock_file_data: ElasticsearchBatchFileData,
    mock_async_client: AsyncMock,
    tmp_path: Path,
):
    """
    Test that when a single field is specified, _source IS included correctly.
    """
    test_fields = ["title"]
    download_config = ElasticsearchDownloaderConfig(
        download_dir=tmp_path,
        fields=test_fields,
    )
    downloader = ElasticsearchDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    captured_query = None

    async def capture_query_async_scan(client, query, scroll, index):
        nonlocal captured_query
        captured_query = query
        async for doc in mock_async_scan(client, query, scroll, index):
            yield doc

    with (
        patch(
            "unstructured_ingest.processes.connectors.elasticsearch.elasticsearch.AsyncElasticsearch",
            return_value=mock_async_client,
        ),
        patch(
            "unstructured_ingest.processes.connectors.elasticsearch.elasticsearch.async_scan",
            side_effect=capture_query_async_scan,
        ),
    ):
        results = await downloader.run_async(file_data=mock_file_data)

        # Verify _source WAS included with the single field
        assert captured_query is not None
        assert "_source" in captured_query
        assert captured_query["_source"] == test_fields
        assert len(captured_query["_source"]) == 1

        # Verify we got download responses
        assert len(results) == 3
