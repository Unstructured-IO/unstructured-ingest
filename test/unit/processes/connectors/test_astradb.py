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
    AstraDBUploadStager,
    AstraDBUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import dependency_exists

# Skip all tests in this module if astrapy is not available
pytestmark = pytest.mark.skipif(
    not dependency_exists("astrapy"),
    reason="astrapy is not installed. Install with: pip install 'unstructured-ingest[astradb]'",
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
    mock_get_collection.with_options = MagicMock(return_value=mock_collection_with_options)

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


def test_astra_generated_embeddings_adds_vectorize_field(file_data: FileData):
    """
    Test that when astra_generated_embeddings=True, the $vectorize field is added
    with the same content as the content field, and $vector is not added.
    """
    stager_config = AstraDBUploadStagerConfig(astra_generated_embeddings=True)
    stager = AstraDBUploadStager(upload_stager_config=stager_config)

    element_dict = {
        "text": "test content",
        "metadata": {"foo": "bar"},
    }

    result = stager.conform_dict(element_dict.copy(), file_data)

    assert "$vectorize" in result
    assert result["$vectorize"] == "test content"
    assert result["content"] == "test content"
    assert result["$vectorize"] == result["content"]
    assert "$vector" not in result


def test_astra_generated_embeddings_default_does_not_add_vectorize_field(file_data: FileData):
    """
    Test that when astra_generated_embeddings is not set (defaults to False),
    the $vectorize field is NOT added, but $vector is added if embeddings exist.
    """
    stager = AstraDBUploadStager()

    element_dict = {
        "text": "test content",
        "embeddings": [0.1, 0.2, 0.3],
        "metadata": {"foo": "bar"},
    }

    result = stager.conform_dict(element_dict.copy(), file_data)

    assert "$vectorize" not in result
    assert "$vector" in result
    assert result["$vector"] == [0.1, 0.2, 0.3]
    assert result["content"] == "test content"


def test_no_embeddings_and_no_astra_generated_raises_error(file_data: FileData):
    """
    Test that when neither embeddings nor astra_generated_embeddings are provided,
    a ValueError is raised.
    """
    stager = AstraDBUploadStager()

    element_dict = {
        "text": "test content",
        "metadata": {"foo": "bar"},
    }

    with pytest.raises(ValueError, match="No vectors provided"):
        stager.conform_dict(element_dict.copy(), file_data)


def test_both_embeddings_and_astra_generated_raises_error(file_data: FileData):
    """
    Test that when both embeddings and astra_generated_embeddings=True are provided,
    a ValueError is raised.
    """
    stager_config = AstraDBUploadStagerConfig(astra_generated_embeddings=True)
    stager = AstraDBUploadStager(upload_stager_config=stager_config)

    element_dict = {
        "text": "test content",
        "embeddings": [0.1, 0.2, 0.3],
        "metadata": {"foo": "bar"},
    }

    with pytest.raises(
        ValueError,
        match="Cannot use Unstructured embeddings and Astra-generated embeddings simultaneously",
    ):
        stager.conform_dict(element_dict.copy(), file_data)


def test_enable_lexical_search_adds_lexical_field(file_data: FileData):
    """
    Test that when enable_lexical_search=True, the $lexical field is added
    with the same content as the content field.
    """
    stager_config = AstraDBUploadStagerConfig(
        enable_lexical_search=True, astra_generated_embeddings=True
    )
    stager = AstraDBUploadStager(upload_stager_config=stager_config)

    element_dict = {
        "text": "test content",
        "metadata": {"foo": "bar"},
    }

    result = stager.conform_dict(element_dict.copy(), file_data)

    assert "$lexical" in result
    assert result["$lexical"] == "test content"
    assert result["content"] == "test content"
    assert result["$lexical"] == result["content"]


def test_enable_lexical_search_default_does_not_add_lexical_field(file_data: FileData):
    """
    Test that when enable_lexical_search is not set (defaults to False),
    the $lexical field is NOT added.
    """
    stager = AstraDBUploadStager()

    element_dict = {
        "text": "test content",
        "embeddings": [0.1, 0.2, 0.3],
        "metadata": {"foo": "bar"},
    }

    result = stager.conform_dict(element_dict.copy(), file_data)

    assert "$lexical" not in result
    assert result["content"] == "test content"


def test_enable_lexical_search_works_with_unstructured_embeddings(file_data: FileData):
    """
    Test that enable_lexical_search=True works correctly with unstructured embeddings.
    """
    stager_config = AstraDBUploadStagerConfig(enable_lexical_search=True)
    stager = AstraDBUploadStager(upload_stager_config=stager_config)

    element_dict = {
        "text": "test content",
        "embeddings": [0.1, 0.2, 0.3],
        "metadata": {"foo": "bar"},
    }

    result = stager.conform_dict(element_dict.copy(), file_data)

    assert "$lexical" in result
    assert result["$lexical"] == "test content"
    assert "$vector" in result
    assert result["$vector"] == [0.1, 0.2, 0.3]
