import sys
import types
from time import time
from types import SimpleNamespace
from unittest import mock

import pytest

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.error import (
    ProviderError,
    RateLimitError,
    SourceConnectionNetworkError,
    UnstructuredIngestError,
    UserAuthError,
    UserError,
    ValueError,
)
from unstructured_ingest.processes.connectors.github import (
    _MAX_RETRY_AFTER_WAIT,
    GithubAccessConfig,
    GithubConnectionConfig,
    GithubDownloader,
    GithubDownloaderConfig,
    GithubIndexer,
    GithubIndexerConfig,
    _GitFile,
    _honor_retry_after,
    _parse_retry_after,
    _rate_limit_backoff,
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


def _patch_github_module():
    """Inject a fake ``github``/``github.GithubException`` so tests don't require the
    optional pygithub extra. Returns the patcher and the fake exception module."""
    github_mod = types.ModuleType("github")
    exc_mod = types.ModuleType("github.GithubException")

    class GithubException(Exception):
        def __init__(self, status=None, data=None, headers=None):
            self.status = status
            self.data = data if data is not None else {}
            self.headers = headers
            super().__init__(str(data))

    class RateLimitExceededException(GithubException):
        pass

    class UnknownObjectException(GithubException):
        pass

    exc_mod.GithubException = GithubException
    exc_mod.RateLimitExceededException = RateLimitExceededException
    exc_mod.UnknownObjectException = UnknownObjectException
    github_mod.GithubException = exc_mod
    patcher = mock.patch.dict(
        sys.modules, {"github": github_mod, "github.GithubException": exc_mod}
    )
    return patcher, exc_mod


def test_wrap_github_exception_unauthorized():
    patcher, _ = _patch_github_module()
    with patcher:
        e = SimpleNamespace(data={"message": "Bad credentials"}, status=401)
        error = _connection_config().wrap_github_exception(e)
    assert isinstance(error, UserAuthError)
    assert "Bad credentials" in str(error)


def test_wrap_github_exception_rate_limit_by_type():
    patcher, exc = _patch_github_module()
    with patcher:
        e = exc.RateLimitExceededException()
        e.data = {"message": "API rate limit exceeded"}
        e.status = 403  # GitHub returns 403 for primary rate limits
        error = _connection_config().wrap_github_exception(e)
    assert isinstance(error, RateLimitError)


def test_wrap_github_exception_forbidden_is_auth_error():
    patcher, _ = _patch_github_module()
    with patcher:
        e = SimpleNamespace(data={"message": "Resource not accessible"}, status=403)
        error = _connection_config().wrap_github_exception(e)
    assert isinstance(error, UserAuthError)


def test_wrap_github_exception_unhandled_returns_original():
    """An unclassified status (e.g. 302) returns the original exception unchanged."""
    patcher, exc = _patch_github_module()
    with patcher:
        e = exc.GithubException(status=302, data={"message": "redirect"})
        wrapped = _connection_config().wrap_github_exception(e)
    assert wrapped is e


def test_wrap_github_exception_non_dict_data():
    """Non-dict e.data must not blow up message extraction."""
    patcher, exc = _patch_github_module()
    with patcher:
        e = exc.GithubException(status=401, data="not-a-dict")
        wrapped = _connection_config().wrap_github_exception(e)
    assert isinstance(wrapped, UserAuthError)


def test_wrap_error_dispatches_github_exception():
    patcher, exc = _patch_github_module()
    with patcher:
        e = exc.GithubException(status=401, data={"message": "Bad creds"})
        wrapped = _connection_config().wrap_error(e)
    assert isinstance(wrapped, UserAuthError)


def test_wrap_error_dispatches_http_error():
    import requests

    patcher, _ = _patch_github_module()
    e = requests.HTTPError(response=SimpleNamespace(status_code=404, text="nope"))
    with patcher:
        wrapped = _connection_config().wrap_error(e)
    assert isinstance(wrapped, UserError)


def test_wrap_error_generic_returns_ingest_error():
    patcher, _ = _patch_github_module()
    with patcher:
        wrapped = _connection_config().wrap_error(RuntimeError("weird"))
    assert isinstance(wrapped, UnstructuredIngestError)


# ---------------------------------------------------------------------------
# Indexer: version, listing, truncation, and API-call economy
# ---------------------------------------------------------------------------


def _indexer(**index_kwargs) -> GithubIndexer:
    return GithubIndexer(
        connection_config=_connection_config(),
        index_config=GithubIndexerConfig(**index_kwargs),
    )


def _entry(path, type="blob", sha="sha", size=1, mode="100644", url="u"):
    return SimpleNamespace(path=path, type=type, sha=sha, size=size, mode=mode, url=url)


class _FakeTree:
    def __init__(self, entries, truncated=False):
        self.tree = entries
        self.truncated = truncated


class _FakeRepo:
    """Records get_git_tree calls and serves trees keyed by tree-ish."""

    def __init__(self, trees, default_branch="main"):
        self._trees = trees
        self.default_branch = default_branch
        self.get_git_tree_calls = []

    def get_git_tree(self, tree_ish, recursive=False):
        self.get_git_tree_calls.append((tree_ish, recursive))
        return self._trees[tree_ish]


def test_convert_element_maps_git_file_to_file_data():
    """convert_element uses the blob SHA as version and carries size/mode/url through."""
    indexer = _indexer()
    git_file = _GitFile(
        path="src/app.py",
        sha="deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        size=123,
        mode="100644",
        url="https://api.github.com/repos/dcneiner/Downloadify/git/blobs/deadbeef",
    )

    file_data = indexer.convert_element(git_file, "main")

    assert file_data.metadata.version == git_file.sha
    assert file_data.metadata.filesize_bytes == 123
    assert file_data.metadata.date_modified is None  # no reliable per-file source
    assert file_data.source_identifiers.relative_path == "src/app.py"
    assert file_data.source_identifiers.fullpath == "main/src/app.py"
    assert file_data.display_name.endswith("/blob/main/src/app.py")


def test_list_files_fast_path_skips_empties_dirs_and_submodules():
    repo = _FakeRepo(
        {
            "main": _FakeTree(
                [
                    _entry("README.md", size=10),
                    _entry("empty.txt", size=0),  # empty file -> skipped
                    _entry("src", type="tree", size=None),  # directory -> skipped
                    _entry("vendored", type="commit", size=None),  # submodule -> skipped
                    _entry("src/app.py", size=42),
                ]
            )
        }
    )
    indexer = _indexer()

    files = indexer.list_files(repo, "main")

    assert sorted(f.path for f in files) == ["README.md", "src/app.py"]
    # Fast path is a single recursive call.
    assert repo.get_git_tree_calls == [("main", True)]


def test_list_files_falls_back_to_per_directory_walk_when_truncated():
    repo = _FakeRepo(
        {
            # Recursive call truncates -> triggers the walk.
            "main": _FakeTree([], truncated=True),
            # Walk: root is fetched non-recursively, then each subtree by sha.
            "main_nonrecursive": _FakeTree(
                [_entry("README.md", size=10), _entry("src", type="tree", sha="src-sha", size=None)]
            ),
            "src-sha": _FakeTree([_entry("app.py", size=42, sha="app-sha")]),
        }
    )

    # get_git_tree is called with the same tree-ish "main" for both the recursive
    # probe and the non-recursive root walk; disambiguate by the recursive flag.
    def get_git_tree(tree_ish, recursive=False):
        repo.get_git_tree_calls.append((tree_ish, recursive))
        if tree_ish == "main":
            return repo._trees["main"] if recursive else repo._trees["main_nonrecursive"]
        return repo._trees[tree_ish]

    repo.get_git_tree = get_git_tree
    indexer = _indexer()

    files = indexer.list_files(repo, "main")

    assert sorted(f.path for f in files) == ["README.md", "src/app.py"]
    assert ("main", True) in repo.get_git_tree_calls  # recursive probe
    assert ("main", False) in repo.get_git_tree_calls  # root walk
    assert ("src-sha", False) in repo.get_git_tree_calls  # subtree walk


def test_run_fetches_repo_only_once(monkeypatch):
    """Regression guard: convert_element must not re-fetch the repo per file."""
    repo = _FakeRepo(
        {"main": _FakeTree([_entry("a.py", size=1, sha="s1"), _entry("b.py", size=1, sha="s2")])}
    )
    calls = {"get_repo": 0}

    def fake_get_repo(self):
        calls["get_repo"] += 1
        return repo

    monkeypatch.setattr(GithubConnectionConfig, "get_repo", fake_get_repo)
    indexer = _indexer()

    file_data = list(indexer.run())

    assert calls["get_repo"] == 1  # not 2N + 1
    assert repo.get_git_tree_calls == [("main", True)]
    assert {fd.metadata.version for fd in file_data} == {"s1", "s2"}


def test_get_branch_prefers_pinned_branch_over_default():
    repo = _FakeRepo({}, default_branch="main")
    assert _indexer().get_branch(repo) == "main"
    assert _indexer(branch="develop").get_branch(repo) == "develop"


def test_convert_element_identifier_is_stable_uuid5():
    """The record identifier must be a deterministic uuid5 of the blob URL so the
    platform can match records across runs (incremental skip depends on it)."""
    from uuid import NAMESPACE_DNS, uuid5

    indexer = _indexer()
    git_file = _GitFile(path="src/app.py", sha="sha", size=1, mode="100644", url="u")

    file_data = indexer.convert_element(git_file, "main")

    expected_url = "https://github.com/dcneiner/Downloadify/blob/main/src/app.py"
    assert file_data.identifier == str(uuid5(NAMESPACE_DNS, expected_url))
    # Stable across repeated conversions of the same file.
    assert indexer.convert_element(git_file, "main").identifier == file_data.identifier
    assert file_data.metadata.permissions_data == [{"mode": "100644"}]


def test_list_files_honors_recursive_false():
    repo = _FakeRepo({"main": _FakeTree([_entry("README.md", size=1)])})

    files = _indexer(recursive=False).list_files(repo, "main")

    assert [f.path for f in files] == ["README.md"]
    assert repo.get_git_tree_calls == [("main", False)]


def test_list_files_recursive_false_truncated_returns_partial():
    """A truncated non-recursive tree can't be paginated or descended into, so the
    connector returns the partial listing (with a warning) rather than walking."""
    repo = _FakeRepo({"main": _FakeTree([_entry("a.txt", size=1)], truncated=True)})

    files = _indexer(recursive=False).list_files(repo, "main")

    assert [f.path for f in files] == ["a.txt"]
    assert repo.get_git_tree_calls == [("main", False)]  # no walk


def test_walk_tree_reconstructs_nested_paths():
    repo = _FakeRepo(
        {
            "root-sha": _FakeTree(
                [
                    _entry("a", type="tree", sha="a-sha", size=None),
                    _entry("top.txt", size=1),
                ]
            ),
            "a-sha": _FakeTree(
                [
                    _entry("b", type="tree", sha="b-sha", size=None),
                    _entry("mid.txt", size=1),
                ]
            ),
            "b-sha": _FakeTree([_entry("deep.txt", size=1)]),
        }
    )

    files = _indexer()._walk_tree(repo, "root-sha", prefix="")

    assert sorted(f.path for f in files) == ["a/b/deep.txt", "a/mid.txt", "top.txt"]


def test_precheck_succeeds_when_repo_reachable(monkeypatch):
    monkeypatch.setattr(GithubConnectionConfig, "get_repo", lambda self: object())
    _indexer().precheck()  # must not raise


def test_precheck_wraps_connection_failure(monkeypatch):
    patcher, _ = _patch_github_module()

    def boom(self):
        raise RuntimeError("cannot reach github")

    monkeypatch.setattr(GithubConnectionConfig, "get_repo", boom)
    with patcher, pytest.raises(UnstructuredIngestError):
        _indexer().precheck()


def test_conform_url_keeps_owner_repo_pair():
    conn = _connection_config()
    assert conn.url == "dcneiner/Downloadify"
    assert conn.get_full_url() == "https://github.com/dcneiner/Downloadify"


# ---------------------------------------------------------------------------
# Downloader: repo caching, fetch, content resolution, and file write
# ---------------------------------------------------------------------------


def _downloader(**download_kwargs) -> GithubDownloader:
    return GithubDownloader(
        download_config=GithubDownloaderConfig(**download_kwargs),
        connection_config=_connection_config(),
    )


def _file_data(path: str = "README.md") -> FileData:
    return _indexer().convert_element(
        _GitFile(path=path, sha="sha", size=1, mode="100644", url="u"), "main"
    )


def test_downloader_caches_repo(monkeypatch):
    """_get_repo memoizes so repeated get_file calls don't re-issue GET /repos."""
    calls = {"n": 0}
    sentinel = object()

    def fake_get_repo(self):
        calls["n"] += 1
        return sentinel

    monkeypatch.setattr(GithubConnectionConfig, "get_repo", fake_get_repo)
    downloader = _downloader()

    assert downloader._get_repo() is sentinel
    assert downloader._get_repo() is sentinel
    assert calls["n"] == 1


def test_get_file_returns_content_file():
    patcher, _ = _patch_github_module()
    downloader = _downloader()
    sentinel = object()
    downloader._repo = SimpleNamespace(get_contents=lambda path: sentinel)
    with patcher:
        assert downloader.get_file(_file_data("README.md")) is sentinel


def test_get_file_missing_raises_user_error():
    patcher, exc = _patch_github_module()
    downloader = _downloader()

    def boom(path):
        raise exc.UnknownObjectException(status=404, data={"message": "Not Found"})

    downloader._repo = SimpleNamespace(get_contents=boom)
    with patcher, pytest.raises(UserError):
        downloader.get_file(_file_data("missing.txt"))


def test_get_contents_returns_decoded_content():
    """Base64-decoded content from the contents API is used without a second fetch."""
    downloader = _downloader()
    content_file = SimpleNamespace(decoded_content=b"hello", download_url="https://raw/x")
    assert downloader.get_contents(content_file) == b"hello"


def test_get_contents_falls_back_to_download_url(monkeypatch):
    """Large files return empty decoded_content, so we fetch the raw download URL."""
    downloader = _downloader()
    content_file = SimpleNamespace(decoded_content=b"", download_url="https://raw/big")
    captured = {}

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured["timeout"] = kwargs.get("timeout")
        return SimpleNamespace(raise_for_status=lambda: None, content=b"raw-bytes")

    monkeypatch.setattr("requests.get", fake_get)

    assert downloader.get_contents(content_file) == b"raw-bytes"
    assert captured["url"] == "https://raw/big"
    # The raw-blob fetch must be bounded so a hung connection can't stall a worker.
    assert captured["timeout"] is not None


def test_get_contents_wraps_http_error(monkeypatch):
    import requests

    patcher, _ = _patch_github_module()
    # max_retries=1: a 500 is now a retriable ProviderError, so a single attempt keeps
    # the test fast while still asserting the HTTPError is wrapped, not leaked raw.
    downloader = _downloader(max_retries=1)
    content_file = SimpleNamespace(decoded_content=None, download_url="https://raw/big")

    def raise_for_status():
        raise requests.HTTPError(response=SimpleNamespace(status_code=500, text="boom"))

    monkeypatch.setattr(
        "requests.get",
        lambda url, **kwargs: SimpleNamespace(raise_for_status=raise_for_status, content=b""),
    )
    with patcher, pytest.raises(ProviderError):
        downloader.get_contents(content_file)


def test_downloader_run_writes_file(tmp_path):
    patcher, _ = _patch_github_module()
    downloader = _downloader(download_dir=tmp_path)
    downloader._repo = SimpleNamespace(
        get_contents=lambda path: SimpleNamespace(decoded_content=b"payload", download_url=None)
    )
    file_data = _file_data("README.md")

    with patcher:
        result = downloader.run(file_data)

    written = tmp_path / "README.md"
    assert written.read_bytes() == b"payload"
    assert result["path"] == written
    assert file_data.local_download_path == str(written.resolve())


def test_downloader_run_creates_parent_directories(tmp_path):
    # Regression guard: run() must mkdir the full parent chain before writing, or
    # files in subdirectories (and the download dir itself) fail with FileNotFoundError.
    patcher, _ = _patch_github_module()
    downloader = _downloader(download_dir=tmp_path)
    downloader._repo = SimpleNamespace(
        get_contents=lambda path: SimpleNamespace(decoded_content=b"x", download_url=None)
    )
    file_data = _file_data("src/app.py")

    with patcher:
        downloader.run(file_data)

    assert (tmp_path / "src" / "app.py").read_bytes() == b"x"


# ---------------------------------------------------------------------------
# Rate limiting & retry: backoff parsing, error classification, retry loop
# ---------------------------------------------------------------------------


@pytest.fixture
def no_sleep(monkeypatch):
    """Make tenacity's backoff instant so retry-loop tests don't actually sleep.

    tenacity's default nap looks up ``time.sleep`` at call time, so patching the time
    module (rather than the bound-at-import ``tenacity.nap.sleep``) is what takes effect.
    """
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)


def test_parse_retry_after_delta_seconds():
    assert _parse_retry_after({"Retry-After": "42"}) == 42.0
    # Header lookup is case-insensitive (PyGithub vs requests casing).
    assert _parse_retry_after({"retry-after": "7"}) == 7.0
    assert _parse_retry_after({}) is None
    assert _parse_retry_after({"Retry-After": "not-a-number"}) is None


def test_parse_retry_after_http_date():
    # An HTTP-date ~30s in the future should resolve to roughly 30s of wait.
    from datetime import datetime, timedelta, timezone
    from email.utils import format_datetime

    future = datetime.now(timezone.utc) + timedelta(seconds=30)
    seconds = _parse_retry_after({"Retry-After": format_datetime(future)})
    assert seconds is not None
    assert 20 <= seconds <= 30


def test_rate_limit_backoff_prefers_retry_after_over_reset():
    reset = str(int(time()) + 999)
    headers = {"Retry-After": "5", "x-ratelimit-remaining": "0", "x-ratelimit-reset": reset}
    assert _rate_limit_backoff(headers) == 5.0


def test_rate_limit_backoff_uses_reset_when_window_exhausted():
    reset = str(int(time()) + 45)
    backoff = _rate_limit_backoff({"x-ratelimit-remaining": "0", "x-ratelimit-reset": reset})
    assert backoff is not None
    assert 40 <= backoff <= 45


def test_rate_limit_backoff_none_when_window_not_exhausted():
    # Remaining budget left -> no primary-limit backoff hint.
    assert _rate_limit_backoff({"x-ratelimit-remaining": "17"}) is None
    assert _rate_limit_backoff({}) is None


def test_honor_retry_after_caps_at_max():
    err = RateLimitError("throttled")
    err.retry_after = 10_000  # absurdly long window
    assert _honor_retry_after(base_seconds=2.0, exc=err) == _MAX_RETRY_AFTER_WAIT


def test_honor_retry_after_falls_back_to_base_when_absent():
    assert _honor_retry_after(base_seconds=3.0, exc=RuntimeError("no hint")) == 3.0


def test_wrap_github_exception_stamps_retry_after_from_header():
    patcher, exc = _patch_github_module()
    with patcher:
        e = exc.RateLimitExceededException(
            status=403, data={"message": "rate limited"}, headers={"Retry-After": "30"}
        )
        err = _connection_config().wrap_github_exception(e)
    assert isinstance(err, RateLimitError)
    assert err.retry_after == 30.0


def test_wrap_github_exception_stamps_retry_after_from_reset():
    patcher, exc = _patch_github_module()
    reset = str(int(time()) + 45)
    with patcher:
        e = exc.RateLimitExceededException(
            status=403,
            data={"message": "rl"},
            headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": reset},
        )
        err = _connection_config().wrap_github_exception(e)
    assert isinstance(err, RateLimitError)
    assert 40 <= err.retry_after <= 45


def test_wrap_http_error_403_rate_limit_is_rate_limit_error():
    reset = str(int(time()) + 30)
    resp = SimpleNamespace(
        status_code=403,
        text="rate limited",
        headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": reset},
    )
    err = _connection_config().wrap_http_error(SimpleNamespace(response=resp))
    assert isinstance(err, RateLimitError)
    assert 0 < err.retry_after <= 30


def test_wrap_http_error_429_is_rate_limit_error():
    resp = SimpleNamespace(status_code=429, text="slow down", headers={"Retry-After": "12"})
    err = _connection_config().wrap_http_error(SimpleNamespace(response=resp))
    assert isinstance(err, RateLimitError)
    assert err.retry_after == 12.0


def test_wrap_error_maps_network_error_to_retriable():
    import requests

    patcher, _ = _patch_github_module()
    with patcher:
        wrapped = _connection_config().wrap_error(requests.ConnectionError("reset by peer"))
    assert isinstance(wrapped, SourceConnectionNetworkError)


def test_run_with_retry_retries_then_succeeds(no_sleep):
    conn = _connection_config()
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RateLimitError("throttled")
        return "ok"

    assert conn.run_with_retry(fn, max_retries=5) == "ok"
    assert calls["n"] == 3


def test_run_with_retry_does_not_retry_user_error(no_sleep):
    conn = _connection_config()
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise UserAuthError("bad token")

    with pytest.raises(UserAuthError):
        conn.run_with_retry(fn, max_retries=5)
    assert calls["n"] == 1  # auth failures are permanent — no retry


def test_run_with_retry_exhausts_and_reraises(no_sleep):
    conn = _connection_config()
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise ProviderError("upstream 500")

    with pytest.raises(ProviderError):
        conn.run_with_retry(fn, max_retries=4)
    assert calls["n"] == 4


def test_run_with_retry_wraps_and_retries_raw_network_error(no_sleep):
    import requests

    patcher, _ = _patch_github_module()
    conn = _connection_config()
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise requests.ConnectionError("reset by peer")

    with patcher, pytest.raises(SourceConnectionNetworkError):
        conn.run_with_retry(fn, max_retries=3)
    assert calls["n"] == 3  # network blips are transient -> retried then reraised


def test_run_with_retry_honors_stamped_retry_after(monkeypatch):
    """The retry wait uses the error's stamped retry_after when it exceeds the base."""
    conn = _connection_config()
    seen_waits = []
    monkeypatch.setattr("time.sleep", lambda seconds, *_a, **_k: seen_waits.append(seconds))

    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] < 2:
            err = RateLimitError("throttled")
            err.retry_after = 25
            raise err
        return "done"

    assert conn.run_with_retry(fn, max_retries=5) == "done"
    assert seen_waits and seen_waits[0] == 25
