from pydantic import SecretStr

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


def test_get_client_without_default_headers(mocker):
    """default_headers=None must not pass the kwarg to the OpenAI constructor."""
    mock_openai = mocker.patch("unstructured_ingest.embed.openai.OpenAI")
    mocker.patch("unstructured_ingest.embed.openai.DefaultHttpxClient")
    config = OpenAIEmbeddingConfig(api_key="key")
    config.get_client()
    _, kwargs = mock_openai.call_args
    assert "default_headers" not in kwargs


def test_get_client_with_default_headers_extracts_secrets(mocker):
    """default_headers values are unwrapped from SecretStr before reaching the SDK."""
    mock_openai = mocker.patch("unstructured_ingest.embed.openai.OpenAI")
    mocker.patch("unstructured_ingest.embed.openai.DefaultHttpxClient")
    config = OpenAIEmbeddingConfig(
        api_key="key",
        default_headers={"X-Custom": SecretStr("token123"), "X-Other": SecretStr("abc")},
    )
    config.get_client()
    _, kwargs = mock_openai.call_args
    assert kwargs["default_headers"] == {"X-Custom": "token123", "X-Other": "abc"}


def test_get_async_client_without_default_headers(mocker):
    """default_headers=None must not pass the kwarg to AsyncOpenAI."""
    mock_async_openai = mocker.patch("unstructured_ingest.embed.openai.AsyncOpenAI")
    mocker.patch("unstructured_ingest.embed.openai.DefaultAsyncHttpxClient")
    config = OpenAIEmbeddingConfig(api_key="key")
    config.get_async_client()
    _, kwargs = mock_async_openai.call_args
    assert "default_headers" not in kwargs


def test_get_async_client_with_default_headers_extracts_secrets(mocker):
    """default_headers values are unwrapped from SecretStr in async client path."""
    mock_async_openai = mocker.patch("unstructured_ingest.embed.openai.AsyncOpenAI")
    mocker.patch("unstructured_ingest.embed.openai.DefaultAsyncHttpxClient")
    config = OpenAIEmbeddingConfig(
        api_key="key",
        default_headers={"Authorization": SecretStr("Bearer tok")},
    )
    config.get_async_client()
    _, kwargs = mock_async_openai.call_args
    assert kwargs["default_headers"] == {"Authorization": "Bearer tok"}
