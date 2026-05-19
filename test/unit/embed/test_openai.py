import sys
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from unstructured_ingest.embed.openai import OpenAIEmbeddingConfig, OpenAIEmbeddingEncoder


@pytest.fixture
def fake_openai(monkeypatch):
    """Inject a stub `openai` module so get_client / get_async_client can be exercised
    without the real `openai` extra installed in the unit-test environment."""
    stub = MagicMock()
    monkeypatch.setitem(sys.modules, "openai", stub)
    return stub


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


def test_get_client_without_default_headers(fake_openai):
    """default_headers=None must not pass the kwarg to the OpenAI constructor."""
    config = OpenAIEmbeddingConfig(api_key="key")
    config.get_client()
    _, kwargs = fake_openai.OpenAI.call_args
    assert "default_headers" not in kwargs


def test_get_client_with_default_headers_extracts_secrets(fake_openai):
    """default_headers values are unwrapped from SecretStr before reaching the SDK."""
    config = OpenAIEmbeddingConfig(
        api_key="key",
        default_headers={"X-Custom": SecretStr("token123"), "X-Other": SecretStr("abc")},
    )
    config.get_client()
    _, kwargs = fake_openai.OpenAI.call_args
    assert kwargs["default_headers"] == {"X-Custom": "token123", "X-Other": "abc"}


def test_get_async_client_without_default_headers(fake_openai):
    """default_headers=None must not pass the kwarg to AsyncOpenAI."""
    config = OpenAIEmbeddingConfig(api_key="key")
    config.get_async_client()
    _, kwargs = fake_openai.AsyncOpenAI.call_args
    assert "default_headers" not in kwargs


def test_get_async_client_with_default_headers_extracts_secrets(fake_openai):
    """default_headers values are unwrapped from SecretStr in async client path."""
    config = OpenAIEmbeddingConfig(
        api_key="key",
        default_headers={"Authorization": SecretStr("Bearer tok")},
    )
    config.get_async_client()
    _, kwargs = fake_openai.AsyncOpenAI.call_args
    assert kwargs["default_headers"] == {"Authorization": "Bearer tok"}


def test_get_client_without_api_key_uses_placeholder(fake_openai):
    """When api_key is None, the SDK is constructed with the literal 'ignored' placeholder
    so header-only OpenAI-compatible gateways can authenticate via default_headers alone."""
    config = OpenAIEmbeddingConfig(
        default_headers={"X-Custom-Token": SecretStr("tenant-key")},
    )
    config.get_client()
    _, kwargs = fake_openai.OpenAI.call_args
    assert kwargs["api_key"] == "ignored"
    assert kwargs["default_headers"] == {"X-Custom-Token": "tenant-key"}


def test_get_async_client_without_api_key_uses_placeholder(fake_openai):
    """Async path mirrors sync: api_key=None plus default_headers triggers the placeholder."""
    config = OpenAIEmbeddingConfig(
        default_headers={"X-Custom-Token": SecretStr("tenant-key")},
    )
    config.get_async_client()
    _, kwargs = fake_openai.AsyncOpenAI.call_args
    assert kwargs["api_key"] == "ignored"
    assert kwargs["default_headers"] == {"X-Custom-Token": "tenant-key"}
