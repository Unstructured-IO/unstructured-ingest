import sys
import types
from types import SimpleNamespace
from unittest import mock

import pytest

from unstructured_ingest.error import (
    ProviderError,
    RateLimitError,
    UnstructuredIngestError,
    UserAuthError,
    UserError,
    ValueError,
)
from unstructured_ingest.processes.connectors.github import (
    GithubAccessConfig,
    GithubConnectionConfig,
    GithubIndexer,
    GithubIndexerConfig,
)

REPO = "dcneiner/Downloadify"


def _connection_config() -> GithubConnectionConfig:
    return GithubConnectionConfig(
        access_config=GithubAccessConfig(access_token="pat-token"),
        url=REPO,
    )


def test_connection_config_pat_auth():
    """Existing PAT (access_token) configuration continues to work unchanged."""
    config = GithubConnectionConfig(
        access_config=GithubAccessConfig(access_token="pat-token"),
        url=REPO,
    )
    assert config.access_config.get_secret_value().access_token == "pat-token"


def test_connection_config_oauth_auth():
    """OAuth configuration (oauth_token + refresh_token) is accepted."""
    config = GithubConnectionConfig(
        access_config=GithubAccessConfig(
            oauth_token="oauth-token",
            refresh_token="refresh-token",
        ),
        url=REPO,
    )
    access_configs = config.access_config.get_secret_value()
    assert access_configs.oauth_token == "oauth-token"
    assert access_configs.refresh_token == "refresh-token"


def test_connection_config_no_auth():
    with pytest.raises(ValueError):
        GithubConnectionConfig(access_config=GithubAccessConfig(), url=REPO)


def test_connection_config_pat_and_oauth_are_exclusive():
    with pytest.raises(ValueError):
        GithubConnectionConfig(
            access_config=GithubAccessConfig(
                access_token="pat-token",
                oauth_token="oauth-token",
            ),
            url=REPO,
        )


def _client_token(access_config: GithubAccessConfig) -> str:
    """Build the client with the github module mocked and return the token passed to it."""
    github_module = mock.MagicMock()
    with mock.patch.dict(sys.modules, {"github": github_module}):
        config = GithubConnectionConfig(access_config=access_config, url=REPO)
        config.get_client()
    _, kwargs = github_module.Github.call_args
    return kwargs["login_or_token"]


def test_get_client_uses_access_token_for_pat():
    assert _client_token(GithubAccessConfig(access_token="pat-token")) == "pat-token"


def test_get_client_uses_oauth_token_when_present():
    assert (
        _client_token(GithubAccessConfig(oauth_token="oauth-token", refresh_token="refresh-token"))
        == "oauth-token"
    )


# ---------------------------------------------------------------------------
# Error classification (AC #7)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status_code,expected_type",
    [
        (401, UserAuthError),  # invalid / expired / revoked token -> reconnect required
        (403, UserAuthError),  # missing scope / insufficient permission -> reconnect required
        (429, RateLimitError),  # rate limited
        (404, UserError),  # generic client error
        (422, UserError),
        (500, ProviderError),  # exactly 500 must classify (regression: was `> 500`)
        (502, ProviderError),
    ],
)
def test_error_for_status_classification(status_code, expected_type):
    error = _connection_config()._error_for_status(status_code, "boom")
    assert isinstance(error, expected_type)


def test_error_for_status_returns_none_when_unhandled():
    assert _connection_config()._error_for_status(200, "ok") is None


def test_wrap_http_error_maps_forbidden_to_auth_error():
    fake = SimpleNamespace(response=SimpleNamespace(status_code=403, text="requires scope repo"))
    error = _connection_config().wrap_http_error(fake)
    assert isinstance(error, UserAuthError)


def test_wrap_http_error_unhandled_falls_back():
    fake = SimpleNamespace(response=SimpleNamespace(status_code=302, text="redirect"))
    error = _connection_config().wrap_http_error(fake)
    assert isinstance(error, UnstructuredIngestError)


def _patch_github_exceptions():
    """Inject a fake github.GithubException module exposing RateLimitExceededException."""
    github_mod = types.ModuleType("github")
    exc_mod = types.ModuleType("github.GithubException")

    class RateLimitExceededException(Exception):
        pass

    exc_mod.RateLimitExceededException = RateLimitExceededException
    github_mod.GithubException = exc_mod
    patcher = mock.patch.dict(
        sys.modules, {"github": github_mod, "github.GithubException": exc_mod}
    )
    return patcher, RateLimitExceededException


def test_wrap_github_exception_unauthorized():
    patcher, _ = _patch_github_exceptions()
    with patcher:
        e = SimpleNamespace(data={"message": "Bad credentials"}, status=401)
        error = _connection_config().wrap_github_exception(e)
    assert isinstance(error, UserAuthError)
    assert "Bad credentials" in str(error)


def test_wrap_github_exception_rate_limit_by_type():
    patcher, RateLimitExceededException = _patch_github_exceptions()
    with patcher:
        e = RateLimitExceededException()
        e.data = {"message": "API rate limit exceeded"}
        e.status = 403  # GitHub returns 403 for primary rate limits
        error = _connection_config().wrap_github_exception(e)
    assert isinstance(error, RateLimitError)


def test_wrap_github_exception_forbidden_is_auth_error():
    patcher, _ = _patch_github_exceptions()
    with patcher:
        e = SimpleNamespace(data={"message": "Resource not accessible"}, status=403)
        error = _connection_config().wrap_github_exception(e)
    assert isinstance(error, UserAuthError)


# ---------------------------------------------------------------------------
# metadata.version / date_modified
# ---------------------------------------------------------------------------


def _indexer() -> GithubIndexer:
    return GithubIndexer(
        connection_config=_connection_config(),
        index_config=GithubIndexerConfig(),
    )


def _tree_element(**overrides) -> SimpleNamespace:
    element = SimpleNamespace(
        path="src/app.py",
        url="https://api.github.com/repos/dcneiner/Downloadify/git/blobs/deadbeef",
        sha="deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        size=123,
        mode="100644",
        last_modified_datetime=None,
    )
    for key, value in overrides.items():
        setattr(element, key, value)
    return element


def test_convert_element_uses_blob_sha_as_version(monkeypatch):
    """metadata.version must be the per-file git blob SHA (not the shared tree ETag)."""
    indexer = _indexer()
    monkeypatch.setattr(indexer, "get_branch", lambda: "main")
    element = _tree_element()

    file_data = indexer.convert_element(element)

    assert file_data.metadata.version == element.sha


def test_convert_element_null_guards_missing_date_modified(monkeypatch):
    """A missing Last-Modified header (None) must not crash indexing."""
    indexer = _indexer()
    monkeypatch.setattr(indexer, "get_branch", lambda: "main")
    element = _tree_element(last_modified_datetime=None)

    file_data = indexer.convert_element(element)

    assert file_data.metadata.date_modified is None


def test_convert_element_sets_date_modified_when_present(monkeypatch):
    from datetime import datetime, timezone

    indexer = _indexer()
    monkeypatch.setattr(indexer, "get_branch", lambda: "main")
    modified = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    element = _tree_element(last_modified_datetime=modified)

    file_data = indexer.convert_element(element)

    assert file_data.metadata.date_modified == str(modified.timestamp())
