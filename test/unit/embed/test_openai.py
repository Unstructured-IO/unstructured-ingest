from openai.types.create_embedding_response import CreateEmbeddingResponse, Embedding, Usage

from unstructured_ingest.embed.openai import OpenAIEmbeddingConfig, OpenAIEmbeddingEncoder


def test_embed_documents_does_not_break_element_to_dict(mocker):
    # Mocked client with the desired behavior for embed_documents
    raw_elements = [{"text": f"This is sentence {i + 1}"} for i in range(2)]
    mock_response = CreateEmbeddingResponse(
        data=[
            Embedding(embedding=[1, 2], index=1, object="embedding")
            for i in range(len(raw_elements))
        ],
        model="model",
        object="list",
        usage=Usage(total_tokens=1, prompt_tokens=1),
    )
    mock_client = mocker.MagicMock()
    mock_client.embeddings.create.return_value = mock_response

    # Mock get_client to return our mock_client
    mocker.patch.object(OpenAIEmbeddingConfig, "get_client", return_value=mock_client)

    encoder = OpenAIEmbeddingEncoder(config=OpenAIEmbeddingConfig(api_key="api_key"))

    elements = encoder.embed_documents(
        elements=raw_elements,
    )
    assert len(elements) == 2
    assert elements[0]["text"] == "This is sentence 1"
    assert elements[1]["text"] == "This is sentence 2"
