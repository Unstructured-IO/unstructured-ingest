from unstructured_ingest.embed.vertexai import VertexAIEmbeddingConfig, VertexAIEmbeddingEncoder


def test_embed_documents_does_not_break_element_to_dict(mocker):
    # Mocked client with the desired behavior for embed_documents
    mock_responses = []
    for i in [1, 2]:
        mock_response = mocker.Mock()
        mocker.patch.object(mock_response, "values", i)
        mock_responses.append(mock_response)

    mock_client = mocker.MagicMock()
    mock_client.get_embeddings.return_value = mock_responses

    # Mock create_client to return our mock_client
    mocker.patch.object(VertexAIEmbeddingConfig, "get_client", return_value=mock_client)
    encoder = VertexAIEmbeddingEncoder(config=VertexAIEmbeddingConfig(api_key={"api_key": "value"}))
    raw_elements = [{"text": f"This is sentence {i + 1}"} for i in range(2)]

    elements = encoder.embed_documents(
        elements=raw_elements,
    )
    assert len(elements) == 2
    assert elements[0]["text"] == "This is sentence 1"
    assert elements[1]["text"] == "This is sentence 2"
