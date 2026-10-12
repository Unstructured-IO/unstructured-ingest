import asyncio
import importlib.util
import json
import os
import urllib.parse
from types import SimpleNamespace

import pytest

from unstructured_ingest.embed.vertexai import (
    AsyncVertexAIEmbeddingEncoder,
    VertexAIEmbeddingConfig,
    VertexAIEmbeddingEncoder,
)
from unstructured_ingest.error import UserAuthError, UserError


def _installed(module: str) -> bool:
    try:
        return importlib.util.find_spec(module) is not None
    except ModuleNotFoundError:
        return False


pytestmark = pytest.mark.skipif(
    not (_installed("google.genai") and _installed("google.auth")),
    reason="vertexai extra not installed",
)

BROKER = "unstructured-broker@di-project.iam.gserviceaccount.com"
TARGET = "gemini-caller@customer-sa-home.iam.gserviceaccount.com"
PROJECT = "customer-vertex"
REGION = "us-central1"
AUDIENCE = (
    "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/"
    "di-pool/providers/di-provider"
)
STS_URL = "https://sts.googleapis.com/v1/token"
IAM_SCOPE = "https://www.googleapis.com/auth/iam"
CLOUD_PLATFORM = "https://www.googleapis.com/auth/cloud-platform"
PREDICT_BODY = {
    "predictions": [
        {"embeddings": {"values": [0.1, 0.2], "statistics": {"token_count": 1, "truncated": False}}}
    ]
}
_ENV = (
    "GCP_WORKLOAD_IDENTITY_SOURCE",
    "GCP_WORKLOAD_IDENTITY_BROKER_SA",
    "GCP_WORKLOAD_IDENTITY_AUDIENCE",
    "GCP_FEDERATED_TOKEN_FILE",
    "GCP_WORKLOAD_IDENTITY_ALLOWED_PROJECTS",
)


def iam_url(email):
    return (
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
        f"{email}:generateAccessToken"
    )


class FakeResponse:
    def __init__(self, status, payload):
        self.status = status
        self.headers = {"content-type": "application/json"}
        self.data = json.dumps(payload).encode()


def rpc_error(code, status):
    return {"error": {"code": code, "message": "redacted", "status": status}}


class FakeGoogle:
    """Answers google.auth requests by URL and records each call."""

    def __init__(self, **answers):
        self.calls = []
        self.answers = {
            "metadata_info": FakeResponse(
                200, {"email": "jobs@di-project.iam.gserviceaccount.com", "scopes": []}
            ),
            "metadata_token": FakeResponse(
                200, {"access_token": "metadata-token", "expires_in": 3600}
            ),
            "sts": FakeResponse(
                200,
                {
                    "access_token": "federated-token",
                    "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                },
            ),
            "broker": FakeResponse(
                200, {"accessToken": "broker-token", "expireTime": "2099-01-01T00:00:00Z"}
            ),
            "service_account": FakeResponse(
                200, {"accessToken": "customer-token", "expireTime": "2099-01-01T00:00:00Z"}
            ),
        }
        self.answers.update(answers)

    @staticmethod
    def _route(url):
        if "/computeMetadata/v1/" in url:
            return "metadata_token" if "/token" in url else "metadata_info"
        routes = {STS_URL: "sts", iam_url(BROKER): "broker", iam_url(TARGET): "service_account"}
        if url not in routes:
            raise AssertionError(f"unexpected request to {url}")
        return routes[url]

    def __call__(self, url, method="GET", body=None, headers=None, **kwargs):
        name = self._route(url)
        if isinstance(body, bytes):
            body = body.decode()
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except ValueError:
                body = dict(urllib.parse.parse_qsl(body))
        self.calls.append(
            SimpleNamespace(name=name, url=url, method=method, body=body, headers=headers or {})
        )
        answer = self.answers[name]
        if isinstance(answer, BaseException):
            raise answer
        return answer

    def names(self):
        return [call.name for call in self.calls]

    def call(self, name):
        return next(call for call in self.calls if call.name == name)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    from unstructured_ingest.embed import vertexai_identity

    for name in _ENV:
        monkeypatch.delenv(name, raising=False)
    vertexai_identity._cached_credentials.cache_clear()
    yield
    vertexai_identity._cached_credentials.cache_clear()


@pytest.fixture
def gke(monkeypatch):
    monkeypatch.setenv("GCP_WORKLOAD_IDENTITY_SOURCE", "gke_metadata")
    monkeypatch.setenv("GCP_WORKLOAD_IDENTITY_BROKER_SA", BROKER)


@pytest.fixture
def pool(monkeypatch, tmp_path):
    token_file = tmp_path / "token"
    token_file.write_text("projected-jwt")
    monkeypatch.setenv("GCP_WORKLOAD_IDENTITY_SOURCE", "identity_pool")
    monkeypatch.setenv("GCP_WORKLOAD_IDENTITY_BROKER_SA", BROKER)
    monkeypatch.setenv("GCP_WORKLOAD_IDENTITY_AUDIENCE", AUDIENCE)
    monkeypatch.setenv("GCP_FEDERATED_TOKEN_FILE", str(token_file))
    return token_file


@pytest.fixture
def google(mocker):
    """The google.auth transport google-genai refreshes credentials through."""
    fake = FakeGoogle()
    mocker.patch("google.auth.transport.requests.Request", return_value=fake)
    return fake


@pytest.fixture
def wire(mocker):
    """Vertex AI requests that reach the wire; the SDK is real."""
    import httpx
    from google.genai import _api_client

    requests: list = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json=PREDICT_BODY)

    transport = httpx.MockTransport(handler)
    mocker.patch.object(
        _api_client, "SyncHttpxClient", lambda **kw: httpx.Client(transport=transport)
    )
    mocker.patch.object(
        _api_client, "AsyncHttpxClient", lambda **kw: httpx.AsyncClient(transport=transport)
    )
    mocker.patch.object(_api_client, "has_aiohttp", False)
    return requests


def _wi_config(**overrides) -> VertexAIEmbeddingConfig:
    values = {
        "service_account_email": TARGET,
        "project_id": PROJECT,
        "region": REGION,
        "model_name": "gemini-embedding-001",
    }
    values.update(overrides)
    return VertexAIEmbeddingConfig(**values)


def _assert_keyless_request(request, model="gemini-embedding-001") -> None:
    assert request.url.host == f"{REGION}-aiplatform.googleapis.com"
    assert request.url.path.endswith(
        f"/projects/{PROJECT}/locations/{REGION}/publishers/google/models/{model}:predict"
    )
    assert request.headers["authorization"] == "Bearer customer-token"
    assert "x-goog-api-key" not in request.headers


# --- embedding with workload identity ------------------------------------------------------


def test_sync_embedding_impersonates_the_service_account(gke, google, wire):
    result = VertexAIEmbeddingEncoder(config=_wi_config()).embed_query("hello")

    assert result == [0.1, 0.2]
    assert len(wire) == 1
    _assert_keyless_request(wire[0])
    assert google.names() == ["metadata_info", "metadata_token", "broker", "service_account"]


@pytest.mark.asyncio
async def test_async_embedding_impersonates_the_service_account(gke, google, wire):
    config = _wi_config()
    result = await config.embed_batch_async(client=config.get_client(), batch=["a", "b"])

    assert result == [[0.1, 0.2], [0.1, 0.2]]
    assert len(wire) == 2
    for request in wire:
        _assert_keyless_request(request)


def test_legacy_model_names_use_the_genai_predict_route(gke, google, wire, mocker):
    legacy = mocker.patch.object(VertexAIEmbeddingConfig, "get_legacy_client")
    config = _wi_config(model_name="text-embedding-005")

    assert config.uses_genai_transport
    VertexAIEmbeddingEncoder(config=config).embed_query("hello")

    legacy.assert_not_called()
    _assert_keyless_request(wire[0], model="text-embedding-005")


@pytest.mark.parametrize("use_async", [False, True])
def test_ambient_env_credentials_never_reach_the_wire(
    gke, google, wire, mocker, monkeypatch, tmp_path, use_async
):
    adc = tmp_path / "adc.json"
    adc.write_text('{"type": "service_account"}')
    decoys = {
        "GOOGLE_APPLICATION_CREDENTIALS": str(adc),
        "GOOGLE_CLOUD_PROJECT": "env-leak-project",
        "GOOGLE_CLOUD_LOCATION": "europe-west4",
        "VERTEXAI_REGION": "asia-east1",
        "GOOGLE_API_KEY": "decoy-google-api-key",
        "GEMINI_API_KEY": "decoy-gemini-api-key",
        "GOOGLE_GENAI_USE_VERTEXAI": "false",
        "GOOGLE_VERTEX_BASE_URL": "https://env-leak.example/",
    }
    for name, value in decoys.items():
        monkeypatch.setenv(name, value)
    mocker.patch("google.auth.default", side_effect=AssertionError("google.auth.default called"))

    config = _wi_config()
    if use_async:
        asyncio.run(config.embed_batch_async(client=config.get_client(), batch=["a"]))
    else:
        config.embed_batch(client=config.get_client(), batch=["a"])

    _assert_keyless_request(wire[0])
    sent = [str(wire[0].url), *wire[0].headers.values()]
    assert not any(decoy in value for decoy in decoys.values() for value in sent)


def test_no_credentials_file_is_registered(gke, google, wire, monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    VertexAIEmbeddingEncoder(config=_wi_config(model_name="text-embedding-005")).embed_query("a")

    assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ


# --- the credential chain ------------------------------------------------------------------


def test_each_hop_sends_the_exact_generate_access_token_body(gke, google, wire):
    config = _wi_config()
    config.embed_batch(client=config.get_client(), batch=["a"])

    broker, target = google.call("broker"), google.call("service_account")
    assert broker.url == iam_url(BROKER)
    assert broker.method == "POST"
    assert broker.body == {"delegates": [], "scope": [IAM_SCOPE], "lifetime": "3600s"}
    assert broker.headers["authorization"] == "Bearer metadata-token"
    assert target.url == iam_url(TARGET)
    assert target.body == {"delegates": [], "scope": [CLOUD_PLATFORM], "lifetime": "3600s"}
    assert target.headers["authorization"] == "Bearer broker-token"


def test_hops_keep_the_pinned_iamcredentials_url_whatever_google_auth_derives(gke, google, mocker):
    from unstructured_ingest.embed.vertexai_identity import vertex_workload_identity_credentials

    # google.auth picks iamcredentials.mtls.googleapis.com at import under client-cert env
    derived = "https://iamcredentials.mtls.googleapis.com/v1/projects/-/serviceAccounts/{}"
    mocker.patch("google.auth.iam._IAM_ENDPOINT", derived + ":generateAccessToken")

    vertex_workload_identity_credentials(TARGET).refresh(google)

    assert [call.url for call in google.calls[-2:]] == [iam_url(BROKER), iam_url(TARGET)]


def test_identity_pool_exchanges_the_projected_token_at_the_fixed_sts_url(pool, google, wire):
    VertexAIEmbeddingEncoder(config=_wi_config()).embed_query("hello")

    assert google.names() == ["sts", "broker", "service_account"]
    sts = google.call("sts")
    assert sts.body["audience"] == AUDIENCE
    assert sts.body["subject_token"] == "projected-jwt"
    assert sts.body["subject_token_type"] == "urn:ietf:params:oauth:token-type:jwt"
    assert google.call("broker").headers["authorization"] == "Bearer federated-token"
    _assert_keyless_request(wire[0])


def test_gke_source_is_built_without_a_metadata_call(gke, mocker):
    from unstructured_ingest.embed.vertexai_identity import vertex_workload_identity_credentials

    mocker.patch(
        "google.auth.compute_engine._metadata.get",
        side_effect=AssertionError("metadata called while building"),
    )
    vertex_workload_identity_credentials(TARGET)


def test_credentials_are_cached_per_target(gke):
    from unstructured_ingest.embed.vertexai_identity import vertex_workload_identity_credentials

    first = vertex_workload_identity_credentials(TARGET)
    assert vertex_workload_identity_credentials(TARGET) is first
    other = "other-caller@customer-sa-home.iam.gserviceaccount.com"
    assert vertex_workload_identity_credentials(other) is not first


@pytest.mark.parametrize(
    ("answers", "stage"),
    [
        ({"metadata_info": "transport"}, "source"),
        ({"broker": FakeResponse(403, rpc_error(403, "PERMISSION_DENIED"))}, "broker"),
        (
            {"service_account": FakeResponse(403, rpc_error(403, "PERMISSION_DENIED"))},
            "service_account",
        ),
    ],
)
def test_a_failure_is_tagged_with_the_stage_that_raised_it(gke, mocker, answers, stage):
    from google.auth import exceptions as google_exceptions

    from unstructured_ingest.embed.vertexai_identity import vertex_workload_identity_credentials

    answers = {
        name: google_exceptions.TransportError("unreachable") if answer == "transport" else answer
        for name, answer in answers.items()
    }
    fake = FakeGoogle(**answers)
    mocker.patch("google.auth._exponential_backoff.time.sleep")
    with pytest.raises(google_exceptions.GoogleAuthError) as excinfo:
        vertex_workload_identity_credentials(TARGET).refresh(fake)
    assert excinfo.value.vertex_identity_stage == stage


def test_a_broker_failure_never_reaches_the_customer_hop(gke):
    from google.auth import exceptions as google_exceptions

    from unstructured_ingest.embed.vertexai_identity import vertex_workload_identity_credentials

    fake = FakeGoogle(broker=FakeResponse(403, rpc_error(403, "PERMISSION_DENIED")))
    with pytest.raises(google_exceptions.RefreshError):
        vertex_workload_identity_credentials(TARGET).refresh(fake)
    assert "service_account" not in fake.names()


# --- deployment settings -------------------------------------------------------------------


def _assert_refused(excinfo, reason):
    assert excinfo.value.error_reason == reason


def test_unset_source_is_unavailable(google, wire):
    with pytest.raises(UserError) as excinfo:
        _wi_config().get_client()
    _assert_refused(excinfo, "vertex_workload_identity_unavailable")
    assert str(excinfo.value) == (
        "Vertex AI workload identity is not available on this deployment. Contact your "
        "Unstructured account team."
    )
    assert google.calls == [] and wire == []


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("GCP_WORKLOAD_IDENTITY_SOURCE", "application_default"),
        ("GCP_WORKLOAD_IDENTITY_BROKER_SA", ""),
        ("GCP_WORKLOAD_IDENTITY_BROKER_SA", "broker@evil.example/x"),
    ],
)
def test_unusable_deployment_settings_are_unavailable(gke, monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(UserError) as excinfo:
        _wi_config().get_client()
    _assert_refused(excinfo, "vertex_workload_identity_unavailable")


def test_identity_pool_without_its_token_file_is_unavailable(pool):
    pool.unlink()
    with pytest.raises(UserError) as excinfo:
        _wi_config().get_client()
    _assert_refused(excinfo, "vertex_workload_identity_unavailable")


def test_identity_pool_needs_a_pinned_google_audience(pool, monkeypatch):
    monkeypatch.setenv("GCP_WORKLOAD_IDENTITY_AUDIENCE", "//evil.example/projects/1")
    with pytest.raises(UserError) as excinfo:
        _wi_config().get_client()
    _assert_refused(excinfo, "vertex_workload_identity_unavailable")


def test_allowlist_matches_the_service_account_project(gke, google, wire, monkeypatch):
    monkeypatch.setenv(
        "GCP_WORKLOAD_IDENTITY_ALLOWED_PROJECTS", " other-project , customer-sa-home"
    )
    VertexAIEmbeddingEncoder(config=_wi_config()).embed_query("hello")
    assert len(wire) == 1


def test_allowed_vertex_project_does_not_admit_a_foreign_service_account(
    gke, google, wire, monkeypatch
):
    # The Vertex project is allowed, but the service account lives elsewhere.
    monkeypatch.setenv("GCP_WORKLOAD_IDENTITY_ALLOWED_PROJECTS", PROJECT)
    with pytest.raises(UserError) as excinfo:
        _wi_config().get_client()
    _assert_refused(excinfo, "vertex_project_not_allowed")
    assert str(excinfo.value) == (
        f"Service account {TARGET} is in a project this deployment does not allow."
    )
    assert google.calls == [] and wire == []


# --- host and project trust ----------------------------------------------------------------


@pytest.mark.parametrize(
    "model",
    [
        "projects/other-project/locations/us-central1/publishers/google/models/gemini-embedding-001",
        f"projects/{PROJECT}/locations/evil.example#/publishers/google/models/gemini-embedding-001",
        f"projects/{PROJECT}/locations/evil.example?/endpoints/1",
        f"projects/{PROJECT}/locations/x@evil.example/endpoints/1",
        "projects/other-project/locations/us-central1",
    ],
)
def test_resource_paths_outside_the_project_or_region_rules_are_refused(gke, google, wire, model):
    with pytest.raises(UserError) as excinfo:
        _wi_config(model_name=model).get_client()
    _assert_refused(excinfo, "vertex_model_outside_project")
    assert google.calls == [] and wire == []


def test_a_resource_path_in_the_configured_project_is_used(gke, google, wire):
    model = (
        f"projects/{PROJECT}/locations/europe-west4/publishers/google/models/gemini-embedding-001"
    )
    config = _wi_config(model_name=model)
    config.embed_batch(client=config.get_client(), batch=["a"])

    assert wire[0].url.host == "europe-west4-aiplatform.googleapis.com"
    assert wire[0].headers["authorization"] == "Bearer customer-token"


# --- config --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"service_account_email": TARGET, "region": REGION},
        {"project_id": PROJECT, "region": REGION},
        {"service_account_email": TARGET, "project_id": PROJECT},
        {
            "api_key": {"type": "service_account", "project_id": PROJECT},
            "service_account_email": TARGET,
            "project_id": PROJECT,
            "region": REGION,
        },
    ],
)
def test_invalid_auth_combinations_are_rejected(kwargs):
    with pytest.raises(ValueError):
        VertexAIEmbeddingConfig(**kwargs)


@pytest.mark.parametrize(
    "overrides",
    [
        {"service_account_email": "123456789-compute@developer.gserviceaccount.com"},
        {"service_account_email": "customer-sa-home@appspot.gserviceaccount.com"},
        {"service_account_email": f"{TARGET}/evil"},
        {"project_id": "customer/vertex"},
        {"project_id": "evil.example#"},
        {"region": "evil.example#"},
        {"region": "us-central1/x"},
    ],
)
def test_identity_fields_must_match_the_platform_rules(overrides):
    with pytest.raises(ValueError, match="vertexai: "):
        _wi_config(**overrides)


def test_identity_fields_are_stripped():
    config = _wi_config(
        service_account_email=f" {TARGET} ", project_id=f" {PROJECT}", region=f"{REGION} "
    )
    assert (config.service_account_email, config.project_id, config.region) == (
        TARGET,
        PROJECT,
        REGION,
    )


def test_key_path_is_unchanged(gke, mocker):
    from unstructured_ingest.embed import vertexai_identity

    chain = mocker.spy(vertexai_identity, "vertex_workload_identity_credentials")
    mocker.patch(
        "google.oauth2.service_account.Credentials.from_service_account_info",
        return_value=mocker.sentinel.credentials,
    )
    client_cls = mocker.patch("google.genai.Client")
    config = VertexAIEmbeddingConfig(
        api_key={"type": "service_account", "project_id": "key-project"},
        model_name="gemini-embedding-001",
    )

    config.get_genai_client()

    kwargs = client_cls.call_args.kwargs
    assert kwargs["credentials"] is mocker.sentinel.credentials
    assert kwargs["project"] == "key-project"
    assert kwargs["location"] == "global"
    chain.assert_not_called()


# --- precheck ------------------------------------------------------------------------------


@pytest.mark.parametrize("encoder_cls", [VertexAIEmbeddingEncoder, AsyncVertexAIEmbeddingEncoder])
def test_precheck_embeds_once_with_the_impersonated_token(gke, google, wire, encoder_cls):
    encoder_cls(config=_wi_config()).precheck()

    assert len(wire) == 1
    _assert_keyless_request(wire[0])


def test_precheck_surfaces_a_hop_failure_with_its_stage(gke, google, wire):
    google.answers["broker"] = FakeResponse(403, rpc_error(403, "PERMISSION_DENIED"))

    with pytest.raises(UserAuthError) as excinfo:
        VertexAIEmbeddingEncoder(config=_wi_config()).precheck()

    assert excinfo.value.__context__.vertex_identity_stage == "broker"
    assert wire == []


def test_precheck_skips_key_configs(mocker):
    get_client = mocker.patch.object(VertexAIEmbeddingConfig, "get_genai_client")
    config = VertexAIEmbeddingConfig(
        api_key={"type": "service_account", "project_id": "key-project"},
        model_name="gemini-embedding-001",
    )

    VertexAIEmbeddingEncoder(config=config).precheck()

    get_client.assert_not_called()


def test_embedder_config_passes_workload_identity_through():
    from unstructured_ingest.processes.embedder import EmbedderConfig

    encoder = EmbedderConfig(
        embedding_provider="vertexai",
        embedding_model_name="gemini-embedding-001",
        embedding_vertexai_region=REGION,
        embedding_vertexai_service_account_email=TARGET,
        embedding_vertexai_project_id=PROJECT,
    ).get_embedder()

    assert encoder.config.api_key is None
    assert (encoder.config.service_account_email, encoder.config.project_id) == (TARGET, PROJECT)
