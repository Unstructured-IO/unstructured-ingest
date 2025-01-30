from unstructured_ingest.embed.openai import OpenAIEmbeddingConfig, OpenAIEmbeddingEncoder


def test_embed_documents_does_not_break_element_to_dict(mocker):
    # Mocked client with the desired behavior for embed_documents
    raw_elements = [{"text": f"This is sentence {i + 1}"} for i in range(4)]
    mock_response = mocker.MagicMock()
    mock_response_data = []
    for i in range(2):
        mock_response_d = mocker.MagicMock()
        mock_response_d.embedding = [1, 2]
        mock_response_data.append(mock_response_d)
    mock_response.data = mock_response_data
    mock_client = mocker.MagicMock()
    mock_client.embeddings.create.return_value = mock_response

    # Mock get_client to return our mock_client
    mocker.patch.object(OpenAIEmbeddingConfig, "get_client", return_value=mock_client)

    encoder = OpenAIEmbeddingEncoder(config=OpenAIEmbeddingConfig(api_key="api_key", batch_size=2))

    elements = encoder.embed_documents(
        elements=raw_elements,
    )
    assert len(elements) == 4
    assert elements[0]["text"] == "This is sentence 1"
    assert elements[1]["text"] == "This is sentence 2"
    assert mock_client.embeddings.create.call_count == 2
