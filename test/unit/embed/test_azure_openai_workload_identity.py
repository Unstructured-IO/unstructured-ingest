import asyncio
import functools
import importlib
import importlib.util
import time

import pytest

from unstructured_ingest.embed.azure_openai import (
    AsyncAzureOpenAIEmbeddingEncoder,
    AzureOpenAIEmbeddingConfig,
    AzureOpenAIEmbeddingEncoder,
)
from unstructured_ingest.error import UserAuthError, UserError

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("openai") is None
    or importlib.util.find_spec("azure.identity") is None,
    reason="azure-openai extra not installed",
)

ENDPOINT = "https://example-resource.openai.azure.com/"
TOKEN = "entra-access-token"
EMBEDDING_BODY = {
    "object": "list",
    "data": [{"object": "embedding", "index": 0, "embedding": [0.1, 0.2]}],
    "model": "ada",
    "usage": {"prompt_tokens": 1, "total_tokens": 1},
}


class _FakeCredential:
    instances: list = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.scopes: list = []
        _FakeCredential.instances.append(self)

    def get_token(self, *scopes, **kwargs):
        from azure.core.credentials import AccessToken

        self.scopes.append(scopes)
        return AccessToken(TOKEN, int(time.time()) + 3600)


@functools.cache
def _sdk():
    """The SDK's real HTTP client classes and the HTTP library it is built on (httpx or httpx2)."""
    from openai import DefaultAsyncHttpxClient, DefaultHttpxClient

    http = importlib.import_module(DefaultHttpxClient.__mro__[1].__module__.split(".")[0])
    return DefaultHttpxClient, DefaultAsyncHttpxClient, http


@pytest.fixture
def wire(mocker, monkeypatch):
    """Capture requests that reach the wire; the credential is faked, the SDK is real."""
    sync_client, async_client, sdk_http = _sdk()  # resolved before the SDK classes are patched
    _FakeCredential.instances = []
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity")
    monkeypatch.delenv("AZURE_OPENAI_AD_TOKEN", raising=False)
    monkeypatch.delenv("AZURE_OPENAI_API_KEY", raising=False)
    mocker.patch("azure.identity.WorkloadIdentityCredential", _FakeCredential)

    requests: list = []

    def handler(request):
        requests.append(request)
        return sdk_http.Response(200, json=EMBEDDING_BODY)

    transport = sdk_http.MockTransport(handler)
    mocker.patch("openai.DefaultHttpxClient", lambda **kw: sync_client(transport=transport))
    mocker.patch("openai.DefaultAsyncHttpxClient", lambda **kw: async_client(transport=transport))
    return requests


def _wi_config(**overrides) -> AzureOpenAIEmbeddingConfig:
    return AzureOpenAIEmbeddingConfig(
        tenant_id="tenant-1",
        client_id="client-1",
        azure_endpoint=ENDPOINT,
        model_name="my-deployment",
        **overrides,
    )


def _assert_bearer_only(request) -> None:
    assert request.headers["authorization"] == f"Bearer {TOKEN}"
    assert "api-key" not in request.headers
    assert "/openai/deployments/my-deployment/embeddings" in request.url.path


def test_sync_embedding_sends_bearer_token_and_no_api_key(wire):
    config = _wi_config()
    result = AzureOpenAIEmbeddingEncoder(config=config).embed_query("hello")

    assert result == [0.1, 0.2]
    assert len(wire) == 1
    _assert_bearer_only(wire[0])
    cred = _FakeCredential.instances[0]
    assert cred.kwargs == {
        "tenant_id": "tenant-1",
        "client_id": "client-1",
        "token_file_path": "/var/run/secrets/azure/tokens/azure-identity",
    }
    assert cred.scopes == [("https://cognitiveservices.azure.com/.default",)]


@pytest.mark.asyncio
async def test_async_embedding_sends_bearer_token_and_no_api_key(wire):
    config = _wi_config()
    result = (
        await AsyncAzureOpenAIEmbeddingEncoder(config=config)
        .get_client()
        .embeddings.create(input="hello", model="my-deployment")
    )

    assert result.data[0].embedding == [0.1, 0.2]
    assert len(wire) == 1
    _assert_bearer_only(wire[0])


@pytest.mark.parametrize("use_async", [False, True])
def test_ambient_env_credentials_do_not_leak_onto_the_wire(wire, monkeypatch, use_async):
    decoys = {
        "AZURE_OPENAI_AD_TOKEN": "decoy-env-ad-token",
        "AZURE_OPENAI_API_KEY": "decoy-env-api-key",
        "AZURE_OPENAI_KEY": "decoy-env-key",
    }
    for name, value in decoys.items():
        monkeypatch.setenv(name, value)

    if use_async:
        asyncio.run(
            _wi_config().get_async_client().embeddings.create(input="hello", model="my-deployment")
        )
    else:
        _wi_config().get_client().embeddings.create(input="hello", model="my-deployment")

    _assert_bearer_only(wire[0])
    sent = [wire[0].url.query.decode(), *wire[0].headers.values()]
    assert not any(decoy in value for decoy in decoys.values() for value in sent)


def test_precheck_authenticates_with_bearer_token(wire):
    _wi_config().run_precheck()

    assert len(wire) == 1
    _assert_bearer_only(wire[0])


def test_precheck_surfaces_auth_rejection(mocker, wire):
    sync_client, _, sdk_http = _sdk()

    def reject(request):
        return sdk_http.Response(
            401, json={"error": {"code": "401", "message": "Token audience is invalid."}}
        )

    transport = sdk_http.MockTransport(reject)
    mocker.patch("openai.DefaultHttpxClient", lambda **kw: sync_client(transport=transport))

    with pytest.raises(UserAuthError, match="audience"):
        _wi_config().run_precheck()


def test_api_key_path_is_unchanged(wire):
    config = AzureOpenAIEmbeddingConfig(
        api_key="secret-key", azure_endpoint=ENDPOINT, model_name="my-deployment"
    )
    config.get_client().embeddings.create(input="hello", model="my-deployment")

    assert wire[0].headers["api-key"] == "secret-key"
    assert TOKEN not in wire[0].headers.get("authorization", "")
    assert _FakeCredential.instances == []


def test_missing_token_file_env_is_a_user_error(wire, monkeypatch):
    monkeypatch.delenv("AZURE_FEDERATED_TOKEN_FILE")

    with pytest.raises(UserError, match="AZURE_FEDERATED_TOKEN_FILE"):
        _wi_config().get_client()


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"tenant_id": "t"},
        {"client_id": "c"},
        {"api_key": "k", "tenant_id": "t", "client_id": "c"},
        {"api_key": "k", "tenant_id": "t"},
    ],
)
def test_invalid_auth_combinations_are_rejected(kwargs):
    with pytest.raises(ValueError):
        AzureOpenAIEmbeddingConfig(azure_endpoint=ENDPOINT, **kwargs)


def test_embedder_config_passes_workload_identity_through():
    from unstructured_ingest.processes.embedder import EmbedderConfig

    encoder = EmbedderConfig(
        embedding_provider="azure-openai",
        embedding_azure_endpoint=ENDPOINT,
        embedding_azure_tenant_id="tenant-1",
        embedding_azure_client_id="client-1",
    ).get_embedder()

    assert encoder.config.api_key is None
    assert (encoder.config.tenant_id, encoder.config.client_id) == ("tenant-1", "client-1")
