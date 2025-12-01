from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import Secret

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.astradb import (
    CONNECTOR_TYPE,
    AstraDBAccessConfig,
    AstraDBConnectionConfig,
    AstraDBUploader,
    AstraDBUploaderConfig,
)


@pytest.fixture
def connection_config():
    """Provides a minimal AstraDBConnectionConfig for testing."""
    access_config = AstraDBAccessConfig(
        token="test_token",
        api_endpoint="https://test-endpoint.apps.astra.datastax.com",
    )
    return AstraDBConnectionConfig(access_config=Secret(access_config))


@pytest.fixture
def file_data():
    return FileData(
        connector_type=CONNECTOR_TYPE,
        identifier="test_id",
        source_identifiers=SourceIdentifiers(filename="test.txt", fullpath="test.txt"),
    )


@pytest.fixture
def mock_collection():
    """Provides a mock async collection for testing."""
    collection = AsyncMock()
    collection.name = "test_collection"
    collection.insert_many = AsyncMock()
    collection.delete_many = AsyncMock(return_value=MagicMock(deleted_count=0))
    return collection


@pytest.fixture
def mock_get_collection(mock_collection):
    """Patches get_async_astra_collection to return the mock collection."""

    async def mock_get_collection_func(*args, **kwargs):
        return mock_collection

    with patch(
        "unstructured_ingest.processes.connectors.astradb.get_async_astra_collection",
        side_effect=mock_get_collection_func,
    ):
        yield mock_collection


@pytest.mark.asyncio
async def test_binary_encode_vectors_false_calls_with_options(
    connection_config: AstraDBConnectionConfig,
    file_data: FileData,
    mock_get_collection: AsyncMock,
):
    """
    Test that when binary_encode_vectors=False, with_options is called to disable the encoding.
    """
    uploader = AstraDBUploader(
        connection_config=connection_config,
        upload_config=AstraDBUploaderConfig(
            collection_name="test_collection",
            binary_encode_vectors=False,
        ),
    )

    mock_collection_with_options = AsyncMock()
    mock_collection_with_options.name = "test_collection"
    mock_get_collection.with_options.return_value = mock_collection_with_options

    with (
        patch("astrapy.api_options.APIOptions") as mock_api_options,
        patch("astrapy.api_options.SerdesOptions") as mock_serdes_options,
    ):
        mock_serdes_instance = MagicMock()
        mock_serdes_options.return_value = mock_serdes_instance
        mock_api_instance = MagicMock()
        mock_api_options.return_value = mock_api_instance

        await uploader.run_data(
            data=[{"$vector": [0.1, 0.2, 0.3], "content": "test", "metadata": {}}],
            file_data=file_data,
        )

        mock_serdes_options.assert_called_once_with(binary_encode_vectors=False)
        mock_api_options.assert_called_once_with(serdes_options=mock_serdes_instance)
        mock_get_collection.with_options.assert_called_once_with(api_options=mock_api_instance)
        mock_collection_with_options.insert_many.assert_called()


@pytest.mark.asyncio
async def test_binary_encode_vectors_default_does_not_call_with_options(
    connection_config: AstraDBConnectionConfig,
    file_data: FileData,
    mock_get_collection: AsyncMock,
):
    """
    Test that when binary_encode_vectors is not set (defaults to True),
    with_options is NOT called.
    """
    uploader = AstraDBUploader(
        connection_config=connection_config,
        upload_config=AstraDBUploaderConfig(collection_name="test_collection"),
    )

    await uploader.run_data(
        data=[{"$vector": [0.1, 0.2, 0.3], "content": "test", "metadata": {}}],
        file_data=file_data,
    )

    mock_get_collection.with_options.assert_not_called()
    mock_get_collection.insert_many.assert_called()
