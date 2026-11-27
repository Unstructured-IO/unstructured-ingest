import pytest

from unstructured_ingest.embed.voyageai import (
    AsyncVoyageAIEmbeddingEncoder,
    VoyageAIEmbeddingConfig,
    VoyageAIEmbeddingEncoder,
)


def test_embed_documents_does_not_break_element_to_dict(mocker):
    # Mocked client with the desired behavior for embed_documents
    mock_response = mocker.MagicMock()
    mocker.patch.object(mock_response, "embeddings", [1, 2])
    mock_client = mocker.MagicMock()
    mock_client.embed.return_value = mock_response

    # Mock get_client to return our mock_client
    mocker.patch.object(VoyageAIEmbeddingConfig, "get_client", return_value=mock_client)

    encoder = VoyageAIEmbeddingEncoder(
        config=VoyageAIEmbeddingConfig(api_key="api_key", model_name="voyage-law-2")
    )
    raw_elements = [{"text": f"This is sentence {i + 1}"} for i in range(2)]

    elements = encoder.embed_documents(
        elements=raw_elements,
    )
    assert len(elements) == 2
    assert elements[0]["text"] == "This is sentence 1"
    assert elements[1]["text"] == "This is sentence 2"


@pytest.mark.parametrize("model_name", ["voyage-multimodal-3", "voyage-multimodal-3.5"])
def test_multimodal_models_use_multimodal_endpoint(mocker, model_name):
    client = mocker.MagicMock()
    client.multimodal_embed.return_value.embeddings = [[1.0]]
    encoder = VoyageAIEmbeddingEncoder(
        config=VoyageAIEmbeddingConfig(api_key="api_key", model_name=model_name)
    )

    assert encoder.embed_batch(client=client, batch=["text"]) == [[1.0]]
    client.multimodal_embed.assert_called_once_with(inputs=[["text"]], model=model_name)
    client.embed.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("model_name", ["voyage-multimodal-3", "voyage-multimodal-3.5"])
async def test_async_multimodal_models_use_multimodal_endpoint(mocker, model_name):
    client = mocker.MagicMock()
    client.multimodal_embed = mocker.AsyncMock()
    client.multimodal_embed.return_value.embeddings = [[1.0]]
    encoder = AsyncVoyageAIEmbeddingEncoder(
        config=VoyageAIEmbeddingConfig(api_key="api_key", model_name=model_name)
    )

    assert await encoder.embed_batch(client=client, batch=["text"]) == [[1.0]]
    client.multimodal_embed.assert_awaited_once_with(inputs=[["text"]], model=model_name)
    client.embed.assert_not_called()
