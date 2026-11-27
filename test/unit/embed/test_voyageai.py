import pytest

from unstructured_ingest.embed.voyageai import VoyageAIEmbeddingConfig, VoyageAIEmbeddingEncoder


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


@pytest.mark.parametrize("error_name", ["Timeout", "APIConnectionError"])
def test_wrap_error_without_http_status_returns_original_error(error_name):
    voyage_error = pytest.importorskip("voyageai.error")
    error = getattr(voyage_error, error_name)("request failed")
    config = VoyageAIEmbeddingConfig(api_key="api_key")

    assert config.wrap_error(error) is error
