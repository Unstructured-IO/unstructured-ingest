from unstructured_ingest.embed.ollama import OllamaEmbeddingConfig, OllamaEmbeddingEncoder


def test_embed_documents_does_not_break_element_to_dict(mocker):
    # Mocked client with the desired behavior for embed_documents
    mock_response = mocker.MagicMock()
    mocker.patch.object(mock_response, "embeddings", [1, 2])
    mock_client = mocker.MagicMock()
    mock_client.embed.return_value = mock_response

    # Mock get_client to return our mock_client
    mocker.patch.object(OllamaEmbeddingConfig, "get_client", return_value=mock_client)

    encoder = OllamaEmbeddingEncoder(
        config=OllamaEmbeddingConfig(model_name="all-minilm")
    )
    raw_elements = [{"text": f"This is sentence {i+1}"} for i in range(2)]

    elements = encoder.embed_documents(
        elements=raw_elements,
    )
    assert len(elements) == 2
    assert elements[0]["text"] == "This is sentence 1"
    assert elements[1]["text"] == "This is sentence 2"
