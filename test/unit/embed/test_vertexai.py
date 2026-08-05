import asyncio

import pytest

from unstructured_ingest.embed.vertexai import (
    AsyncVertexAIEmbeddingEncoder,
    VertexAIEmbeddingConfig,
    VertexAIEmbeddingEncoder,
    _vertex_multi_region_base_url,
)
from unstructured_ingest.error import ProviderError, UserError

CREDENTIALS = {"project_id": "test-project", "type": "service_account"}


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


# --- transport routing -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("model_name", "expected"),
    [
        ("gemini-embedding-2", True),
        ("gemini-embedding-001", True),
        ("GEMINI-EMBEDDING-2", True),
        ("text-embedding-005", False),
        ("text-embedding-004", False),
        ("textembedding-gecko@001", False),
        # A fine-tune resource path is not a Gemini embedding id and keeps the legacy transport.
        ("projects/p/locations/us-east1/endpoints/123", False),
    ],
)
def test_uses_genai_transport(model_name: str, expected: bool):
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name=model_name)
    assert config.uses_genai_transport is expected


def test_get_client_routes_gemini_to_genai(mocker):
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name="gemini-embedding-2")
    genai = mocker.patch.object(VertexAIEmbeddingConfig, "get_genai_client")
    legacy = mocker.patch.object(VertexAIEmbeddingConfig, "get_legacy_client")

    config.get_client()

    genai.assert_called_once()
    legacy.assert_not_called()


def test_get_client_routes_legacy_model_to_text_embedding_model(mocker):
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name="text-embedding-005")
    genai = mocker.patch.object(VertexAIEmbeddingConfig, "get_genai_client")
    legacy = mocker.patch.object(VertexAIEmbeddingConfig, "get_legacy_client")

    config.get_client()

    legacy.assert_called_once()
    genai.assert_not_called()


# --- genai embed calls -----------------------------------------------------------------------


def _genai_client(mocker, values: list[list[float]]):
    """A genai client whose embed_content returns one embedding per call, in order."""
    client = mocker.MagicMock()

    def respond(**kwargs):
        response = mocker.MagicMock()
        response.embeddings = [mocker.MagicMock(values=values[respond.calls])]
        respond.calls += 1
        return response

    respond.calls = 0
    client.models.embed_content.side_effect = respond
    return client


def test_genai_embed_batch_sends_one_request_per_text(mocker):
    """embedContent takes exactly one content per request: a list of strings is coalesced into a
    single multi-part Content and returns one vector for the whole batch. Fan out instead."""
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name="gemini-embedding-2")
    client = _genai_client(mocker, [[0.1, 0.2], [0.3, 0.4]])

    result = config.embed_batch(client=client, batch=["a", "b"])

    assert result == [[0.1, 0.2], [0.3, 0.4]]
    assert client.models.embed_content.call_count == 2
    sent = [call.kwargs["contents"] for call in client.models.embed_content.call_args_list]
    assert sent == ["a", "b"], "each text must be sent as its own single content"
    first = client.models.embed_content.call_args_list[0].kwargs
    assert first["model"] == "gemini-embedding-2"
    # No dimensionality/task configured -> no config object is sent
    assert first["config"] is None


def test_genai_embed_batch_accepts_a_tuple_batch(mocker):
    """batch_generator yields tuples, so the transport must not assume a list."""
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name="gemini-embedding-2")
    client = _genai_client(mocker, [[0.1], [0.2]])

    result = config.embed_batch(client=client, batch=("a", "b"))

    assert result == [[0.1], [0.2]]


def test_genai_embed_batch_raises_when_no_embedding_returned(mocker):
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name="gemini-embedding-2")
    client = mocker.MagicMock()
    response = mocker.MagicMock()
    response.embeddings = []
    client.models.embed_content.return_value = response

    with pytest.raises(ProviderError, match="no embedding"):
        config.embed_batch(client=client, batch=["a"])


def test_genai_embed_batch_forwards_dimensionality_and_task(mocker):
    config = VertexAIEmbeddingConfig(
        api_key=CREDENTIALS,
        model_name="gemini-embedding-2",
        dimensionality=768,
        task="RETRIEVAL_DOCUMENT",
    )
    client = _genai_client(mocker, [[0.1]])

    config.embed_batch(client=client, batch=["a"])

    sent = client.models.embed_content.call_args.kwargs["config"]
    assert sent.output_dimensionality == 768
    assert sent.task_type == "RETRIEVAL_DOCUMENT"


def test_genai_embed_batch_forwards_zero_dimensionality(mocker):
    """0 is a configured value, not "unset" — forward it and let the provider reject it, rather
    than silently sending the model's full-width default."""
    config = VertexAIEmbeddingConfig(
        api_key=CREDENTIALS, model_name="gemini-embedding-2", dimensionality=0
    )
    client = _genai_client(mocker, [[0.1]])

    config.embed_batch(client=client, batch=["a"])

    sent = client.models.embed_content.call_args.kwargs["config"]
    assert sent is not None
    assert sent.output_dimensionality == 0


def test_legacy_embed_batch_uses_text_embedding_model(mocker):
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name="text-embedding-005")
    client = mocker.MagicMock()
    client.get_embeddings.return_value = [mocker.MagicMock(values=[0.7])]

    result = config.embed_batch(client=client, batch=["a"])

    assert result == [[0.7]]
    client.get_embeddings.assert_called_once()
    client.models.embed_content.assert_not_called()


@pytest.mark.asyncio
async def test_genai_embed_batch_async_fans_out_preserving_order(mocker):
    """Concurrent per-text requests must still come back aligned with the input order."""
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name="gemini-embedding-2")
    client = mocker.MagicMock()
    vectors = {"a": [0.1], "b": [0.2], "c": [0.3]}
    sent = []

    async def embed_content(**kwargs):
        text = kwargs["contents"]
        sent.append(text)
        # Resolve out of input order so a mis-ordered gather would be caught.
        await asyncio.sleep(0.01 if text == "a" else 0)
        response = mocker.MagicMock()
        response.embeddings = [mocker.MagicMock(values=vectors[text])]
        return response

    client.aio.models.embed_content = embed_content

    result = await config.embed_batch_async(client=client, batch=("a", "b", "c"))

    assert result == [[0.1], [0.2], [0.3]]
    assert sorted(sent) == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_async_encoder_delegates_to_config(mocker):
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name="gemini-embedding-2")
    encoder = AsyncVertexAIEmbeddingEncoder(config=config)
    mocker.patch.object(VertexAIEmbeddingConfig, "get_client", return_value=mocker.MagicMock())
    embed = mocker.patch.object(
        VertexAIEmbeddingConfig,
        "embed_batch_async",
        return_value=[[0.1], [0.2]],
    )

    elements = await encoder.embed_documents(elements=[{"text": "a"}, {"text": "b"}])

    embed.assert_awaited_once()
    assert [e["embeddings"] for e in elements] == [[0.1], [0.2]]


# --- region resolution -----------------------------------------------------------------------


def test_region_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("VERTEXAI_REGION", "us-east1")
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name="gemini-embedding-2")
    assert config._resolve_location() == "us-east1"


def test_explicit_region_wins_over_env(monkeypatch):
    monkeypatch.setenv("VERTEXAI_REGION", "us-east1")
    config = VertexAIEmbeddingConfig(
        api_key=CREDENTIALS, model_name="gemini-embedding-2", region="europe-west4"
    )
    assert config._resolve_location() == "europe-west4"


def test_resource_path_region_wins_over_config_and_env(monkeypatch):
    monkeypatch.setenv("VERTEXAI_REGION", "us-east1")
    config = VertexAIEmbeddingConfig(
        api_key=CREDENTIALS,
        model_name="projects/p/locations/europe-west1/endpoints/123",
        region="us-central1",
    )
    assert config._resolve_location() == "europe-west1"


def test_missing_region_raises_user_error(monkeypatch):
    monkeypatch.delenv("VERTEXAI_REGION", raising=False)
    config = VertexAIEmbeddingConfig(api_key=CREDENTIALS, model_name="gemini-embedding-2")
    with pytest.raises(UserError, match="Vertex AI region is required"):
        config._resolve_location()


def test_genai_client_requires_project_id(monkeypatch):
    monkeypatch.setenv("VERTEXAI_REGION", "us-east1")
    config = VertexAIEmbeddingConfig(
        api_key={"type": "service_account"}, model_name="gemini-embedding-2"
    )
    with pytest.raises(UserError, match="project_id"):
        config.get_genai_client()


@pytest.mark.parametrize(
    ("location", "expected_base_url"),
    [
        ("us", "https://aiplatform.us.rep.googleapis.com/"),
        ("eu", "https://aiplatform.eu.rep.googleapis.com/"),
        ("us-east1", None),
    ],
)
def test_multi_region_base_url(location: str, expected_base_url):
    assert _vertex_multi_region_base_url(location) == expected_base_url
