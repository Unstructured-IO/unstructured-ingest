from unstructured_ingest.embed.octoai import OctoAiEmbeddingConfig, OctoAIEmbeddingEncoder


def test_embed_documents_does_not_break_element_to_dict(mocker):
    # Mocked client with the desired behavior for embed_documents
    mock_client = mocker.MagicMock()
    mock_client.embed_documents.return_value = [1, 2]

    # Mock get_client to return our mock_client
    mocker.patch.object(OctoAiEmbeddingConfig, "get_client", return_value=mock_client)

    encoder = OctoAIEmbeddingEncoder(config=OctoAiEmbeddingConfig(api_key="api_key"))
    raw_elements = [{"text": f"This is sentence {i+1}"} for i in range(2)]

    elements = encoder.embed_documents(
        elements=raw_elements,
    )
    assert len(elements) == 2
    assert elements[0]["text"] == "This is sentence 1"
    assert elements[1]["text"] == "This is sentence 2"
