import builtins
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Callable, Generator, Optional
from urllib.parse import urlparse
from uuid import NAMESPACE_DNS, uuid5

from pydantic import Field, Secret, field_validator

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import (
    ProviderError,
    RateLimitError,
    SourceConnectionNetworkError,
    UnstructuredIngestError,
    UserAuthError,
    UserError,
    ValueError,
    safe_error_summary,
)
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    Indexer,
    IndexerConfig,
    download_responses,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    SourceRegistryEntry,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from github import ContentFile, GitTreeElement, Repository
    from github import Github as GithubClient
    from github.GithubException import GithubException
    from requests import HTTPError

CONNECTOR_TYPE = "github"

# Cap on honored backoff: GitHub's primary-limit reset can be an hour out, and we
# won't stall a run that long on one Retry-After / X-RateLimit-Reset.
_MAX_RETRY_AFTER_WAIT = 300.0


def _header_value(headers: Any, name: str) -> Optional[str]:
    """Read a header case-insensitively (PyGithub and requests differ on key casing)."""
    if not headers:
        return None
    try:
        if name in headers:
            return headers[name]
        lname = name.lower()
        for key, value in headers.items():
            if key.lower() == lname:
                return value
    except (builtins.TypeError, builtins.AttributeError):
        return None
    return None


def _parse_retry_after(headers: Any) -> Optional[float]:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date) into seconds."""
    value = _header_value(headers, "Retry-After")
    if not value:
        return None
    # This module shadows the builtin ``ValueError``, so catch builtins explicitly.
    try:
        return max(0.0, float(value))
    except (builtins.TypeError, builtins.ValueError):
        pass
    try:
        from datetime import datetime, timezone
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(value)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except (builtins.TypeError, builtins.ValueError):
        pass
    return None


def _rate_limit_backoff(headers: Any) -> Optional[float]:
    """Seconds to wait before retrying a throttle: prefer the secondary-limit
    ``Retry-After``, else the primary-limit ``x-ratelimit-reset`` once the window is
    exhausted (``x-ratelimit-remaining: 0``). ``None`` when no hint is available."""
    retry_after = _parse_retry_after(headers)
    if retry_after is not None:
        return retry_after
    if str(_header_value(headers, "x-ratelimit-remaining")) == "0":
        reset = _header_value(headers, "x-ratelimit-reset")
        if reset is not None:
            try:
                return max(0.0, float(reset) - time())
            except (builtins.TypeError, builtins.ValueError):
                return None
    return None


def _honor_retry_after(base_seconds: float, exc: BaseException) -> float:
    """Use the server's stamped ``retry_after`` when it exceeds the backoff, capped."""
    retry_after = getattr(exc, "retry_after", None)
    if isinstance(retry_after, (int, float)) and retry_after > base_seconds:
        return min(float(retry_after), _MAX_RETRY_AFTER_WAIT)
    return base_seconds


def _should_retry(exc: BaseException) -> bool:
    """Retry throttles, transient 5xx, and network blips — never auth/user errors."""
    return isinstance(exc, (RateLimitError, ProviderError, SourceConnectionNetworkError))


class GithubAccessConfig(AccessConfig):
    access_token: Optional[str] = Field(
        description="Github personal access token (PAT)",
        default=None,
    )
    oauth_token: Optional[str] = Field(
        description="GitHub OAuth 2.0 access token",
        default=None,
    )
    refresh_token: Optional[str] = Field(
        description=(
            "GitHub OAuth 2.0 refresh token. Used by the platform to refresh expired "
            "access tokens before each job run; the connector never uses it directly."
        ),
        default=None,
    )


class GithubConnectionConfig(ConnectionConfig):
    access_config: Secret[GithubAccessConfig]
    url: str = Field(description="Github url or repository owner/name pair")

    @field_validator("url", mode="after")
    def conform_url(cls, value: str):
        parsed_url = urlparse(value)
        return parsed_url.path

    def model_post_init(self, __context):
        access_configs = self.access_config.get_secret_value()
        pat_auth = bool(access_configs.access_token)
        oauth_auth = bool(access_configs.oauth_token)
        if pat_auth and oauth_auth:
            raise ValueError(
                "access_token (PAT) and oauth_token are mutually exclusive, provide only one"
            )
        if not (pat_auth or oauth_auth):
            raise ValueError("no form of auth provided, set access_token or oauth_token")

    def get_full_url(self):
        return f"https://github.com/{self.url}"

    @requires_dependencies(["github"], extras="github")
    def get_client(self) -> "GithubClient":
        from github import Github as GithubClient

        access_configs = self.access_config.get_secret_value()
        # The platform's init-oauth-refresh container populates oauth_token with a fresh
        # access token before the job runs (per PLU-381); refresh is not handled here.
        token = access_configs.oauth_token or access_configs.access_token
        return GithubClient(login_or_token=token)

    def get_repo(self) -> "Repository":
        client = self.get_client()
        return client.get_repo(self.url)

    def _error_for_status(self, status_code: int, message: str) -> Optional[Exception]:
        """Map a GitHub HTTP status to a classified error, or None if unhandled.

        401 (invalid/expired/revoked token) and 403 (missing scope / insufficient
        permission) both map to UserAuthError so the platform prompts a reconnect.
        """
        if status_code == 429:
            return RateLimitError(f"GitHub API rate limit exceeded: {message}")
        if status_code == 401:
            return UserAuthError(f"Unauthorized access to Github: {message}")
        if status_code == 403:
            return UserAuthError(
                f"Forbidden access to Github (missing scope or insufficient permission): {message}"
            )
        if 400 <= status_code < 500:
            return UserError(message)
        if status_code >= 500:
            return ProviderError(message)
        return None

    def wrap_github_exception(self, e: "GithubException") -> Exception:
        from github.GithubException import RateLimitExceededException

        headers = getattr(e, "headers", None) or {}
        data = e.data if isinstance(e.data, dict) else {}
        message = data.get("message") or safe_error_summary(e)
        # PyGithub raises RateLimitExceededException with a 403 status for primary
        # rate limits, so classify by type before falling back to the status map.
        if isinstance(e, RateLimitExceededException):
            error: Optional[Exception] = RateLimitError(
                f"GitHub API rate limit exceeded: {message}"
            )
        else:
            error = self._error_for_status(e.status, message)
            if error is None:
                logger.debug(f"unhandled github error: {safe_error_summary(e)}")
                return e
        # Stamp the throttle backoff so a retry can honor GitHub's requested window.
        if isinstance(error, RateLimitError):
            backoff = _rate_limit_backoff(headers)
            if backoff is not None:
                error.retry_after = backoff
        return error

    def wrap_http_error(self, e: "HTTPError") -> Exception:
        response = e.response
        headers = getattr(response, "headers", None) or {}
        status_code = response.status_code
        # A 403 whose primary rate-limit window is exhausted is a throttle, not an auth
        # failure — classify it as a (retriable) RateLimitError and honor its backoff.
        rate_limited = status_code == 429 or (
            status_code == 403 and str(_header_value(headers, "x-ratelimit-remaining")) == "0"
        )
        if rate_limited:
            error = RateLimitError(f"GitHub API rate limit exceeded: {response.text}")
            backoff = _rate_limit_backoff(headers)
            if backoff is not None:
                error.retry_after = backoff
            return error
        mapped = self._error_for_status(status_code, response.text)
        if mapped is not None:
            return mapped
        logger.debug(f"unhandled http error: {safe_error_summary(e)}")
        return UnstructuredIngestError(safe_error_summary(e))

    @requires_dependencies(["requests"], extras="github")
    def wrap_error(self, e: Exception) -> Exception:
        from github.GithubException import GithubException
        from requests import ConnectionError as RequestsConnectionError
        from requests import HTTPError, Timeout

        if isinstance(e, GithubException):
            return self.wrap_github_exception(e=e)
        if isinstance(e, HTTPError):
            return self.wrap_http_error(e=e)
        # A connection reset / timeout contacting GitHub is transient — classify it as a
        # retriable network error rather than an opaque failure.
        if isinstance(e, (RequestsConnectionError, Timeout)):
            return SourceConnectionNetworkError(
                f"Network error contacting GitHub: {safe_error_summary(e)}"
            )
        logger.debug(f"unhandled error: {safe_error_summary(e)}")
        return UnstructuredIngestError(safe_error_summary(e))

    @requires_dependencies(["tenacity"], extras="github")
    def run_with_retry(self, fn: Callable[[], Any], *, max_retries: int) -> Any:
        """Run *fn* (a GitHub call), retrying transient throttles / 5xx / network errors
        with exponential backoff that honors GitHub's ``Retry-After``. Failures are
        normalized through ``wrap_error`` so the predicate sees our typed errors."""
        from tenacity import (
            retry,
            retry_if_exception,
            stop_after_attempt,
            wait_exponential,
        )

        exp_wait = wait_exponential(exp_base=2, multiplier=1, min=2, max=30)

        def _wait(retry_state) -> float:
            base = exp_wait(retry_state)
            exc = retry_state.outcome.exception() if retry_state.outcome else None
            return _honor_retry_after(base, exc) if exc is not None else base

        @retry(
            stop=stop_after_attempt(max_retries),
            wait=_wait,
            retry=retry_if_exception(_should_retry),
            reraise=True,
        )
        def _wrapped() -> Any:
            try:
                return fn()
            except UnstructuredIngestError:
                # Already typed (retry_after preserved) — don't re-wrap and lose it.
                raise
            except Exception as e:
                raise self.wrap_error(e=e) from e

        return _wrapped()


@dataclass
class _GitFile:
    """A blob entry resolved to its full repo-relative path, so ``convert_element``
    sees one uniform shape from both the recursive-tree and truncation-walk paths."""

    path: str
    sha: str
    size: int
    mode: str
    url: str


class GithubIndexerConfig(IndexerConfig):
    branch: Optional[str] = Field(
        description="Branch to index, use the default if one isn't provided", default=None
    )
    recursive: bool = Field(
        description="Recursively index all files in the repository", default=True
    )
    max_retries: int = Field(
        description="Max attempts for a GitHub API call before giving up on a "
        "throttle / transient 5xx / network error",
        default=10,
    )


@dataclass
class GithubIndexer(Indexer):
    connection_config: GithubConnectionConfig
    index_config: GithubIndexerConfig = field(default_factory=GithubIndexerConfig)
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        # run_with_retry already normalizes failures to typed errors, so no wrapping here.
        self.connection_config.run_with_retry(
            self.connection_config.get_repo, max_retries=self.index_config.max_retries
        )

    def get_branch(self, repo: "Repository") -> str:
        # No extra call: a pinned branch short-circuits, otherwise default_branch is
        # already populated on the repo object we fetch once in run().
        return self.index_config.branch or repo.default_branch

    def list_files(self, repo: "Repository", branch: str) -> list[_GitFile]:
        """List every non-empty blob under *branch*, robust to tree truncation.

        Fast path: a single recursive git-tree call. GitHub caps the recursive tree
        at ~100k entries / 7 MB and cannot paginate it, so when the response is
        truncated we fall back to a per-directory walk that fetches each subtree
        individually and reassembles full paths.
        """
        recursive = self.index_config.recursive
        git_tree = self.connection_config.run_with_retry(
            lambda: repo.get_git_tree(branch, recursive=recursive),
            max_retries=self.index_config.max_retries,
        )
        if not git_tree.truncated:
            return self._blobs(git_tree.tree)

        if not recursive:
            # A non-recursive listing can't be completed — git trees have no page
            # param and there's no lower level to descend into.
            logger.warning(
                "GitHub root tree for %s is truncated; some files may be missing",
                self.connection_config.url,
            )
            return self._blobs(git_tree.tree)

        logger.warning(
            "GitHub tree for %s is truncated; falling back to a per-directory walk",
            self.connection_config.url,
        )
        return self._walk_tree(repo, branch, prefix="")

    @staticmethod
    def _blobs(entries: list["GitTreeElement"], prefix: str = "") -> list[_GitFile]:
        # Keep only blobs with content: skips trees (directories), submodules
        # (type "commit"), and empty files (size 0 / None).
        files: list[_GitFile] = []
        for e in entries:
            if e.type == "blob" and e.size:
                path = f"{prefix}/{e.path}" if prefix else e.path
                files.append(_GitFile(path=path, sha=e.sha, size=e.size, mode=e.mode, url=e.url))
        return files

    def _walk_tree(self, repo: "Repository", tree_ish: str, prefix: str) -> list[_GitFile]:
        """Collect blobs one directory at a time (recursive-tree truncation fallback)."""
        tree = self.connection_config.run_with_retry(
            lambda: repo.get_git_tree(tree_ish, recursive=False),
            max_retries=self.index_config.max_retries,
        )
        if tree.truncated:
            # A single directory exceeding the per-tree cap is astronomically rare;
            # warn rather than silently drop entries.
            logger.warning(
                "GitHub subtree %r under %s is truncated; some files may be missing",
                prefix or "/",
                self.connection_config.url,
            )
        files = self._blobs(tree.tree, prefix)
        for e in tree.tree:
            if e.type == "tree":
                child_prefix = f"{prefix}/{e.path}" if prefix else e.path
                files.extend(self._walk_tree(repo, e.sha, child_prefix))
        return files

    def convert_element(self, file: _GitFile, branch: str) -> FileData:
        full_path = f"{self.connection_config.get_full_url()}/blob/{branch}/{file.path}"

        return FileData(
            identifier=str(uuid5(NAMESPACE_DNS, full_path)),
            connector_type=self.connector_type,
            display_name=full_path,
            source_identifiers=SourceIdentifiers(
                filename=Path(file.path).name,
                fullpath=(Path(branch) / file.path).as_posix(),
                rel_path=file.path,
            ),
            metadata=FileDataSourceMetadata(
                url=file.url,
                # Blob SHA: a content hash already in the tree listing (no extra call)
                # that changes iff content changes — what the platform's per-record
                # incremental skip compares on. (The tree ETag is shared across files.)
                version=file.sha,
                record_locator={},
                # date_modified is intentionally unset: git tree entries carry no
                # per-file timestamp, and version is the sole skip signal anyway.
                date_processed=str(time()),
                filesize_bytes=file.size,
                permissions_data=[{"mode": file.mode}],
            ),
        )

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        # Fetch the repo (and default_branch) once per run, not per file.
        repo = self.connection_config.run_with_retry(
            self.connection_config.get_repo, max_retries=self.index_config.max_retries
        )
        branch = self.get_branch(repo)
        for file in self.list_files(repo, branch):
            yield self.convert_element(file, branch)


class GithubDownloaderConfig(DownloaderConfig):
    max_retries: int = Field(
        description="Max attempts for a GitHub API call before giving up on a "
        "throttle / transient 5xx / network error",
        default=10,
    )


@dataclass
class GithubDownloader(Downloader):
    download_config: GithubDownloaderConfig
    connection_config: GithubConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    _repo: Optional["Repository"] = field(default=None, init=False, repr=False)

    def _get_repo(self) -> "Repository":
        # Cache the repo so repeated get_file calls in one downloader process don't
        # each re-issue GET /repos/{owner}/{repo}.
        if self._repo is None:
            self._repo = self.connection_config.run_with_retry(
                self.connection_config.get_repo, max_retries=self.download_config.max_retries
            )
        return self._repo

    @requires_dependencies(["github"], extras="github")
    def get_file(self, file_data: FileData) -> "ContentFile":
        from github.GithubException import UnknownObjectException

        path = file_data.source_identifiers.relative_path
        repo = self._get_repo()

        def _fetch() -> "ContentFile":
            try:
                return repo.get_contents(path)
            except UnknownObjectException:
                # A missing file is a permanent user error — surface it without retrying.
                logger.error(f"File doesn't exist: {self.connection_config.url}/{path}")
                raise UserError(f"File not found: {path}")

        return self.connection_config.run_with_retry(
            _fetch, max_retries=self.download_config.max_retries
        )

    @requires_dependencies(["requests"], extras="github")
    def get_contents(self, content_file: "ContentFile") -> bytes:
        import requests

        if content_file.decoded_content:
            return content_file.decoded_content
        download_url = content_file.download_url

        def _fetch() -> bytes:
            # Bound the request so a hung raw-blob fetch can't stall a worker.
            resp = requests.get(download_url, timeout=60)
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                raise self.connection_config.wrap_error(e=e)
            return resp.content

        return self.connection_config.run_with_retry(
            _fetch, max_retries=self.download_config.max_retries
        )

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        content_file = self.get_file(file_data)
        contents = self.get_contents(content_file)
        download_path = self.get_download_path(file_data)
        # Files live under nested paths and the download dir may not exist yet; nothing
        # upstream creates the parent chain, so do it here before writing.
        download_path.parent.mkdir(parents=True, exist_ok=True)
        with download_path.open("wb") as f:
            f.write(contents)
        return self.generate_download_response(file_data=file_data, download_path=download_path)


github_source_entry = SourceRegistryEntry(
    indexer=GithubIndexer,
    indexer_config=GithubIndexerConfig,
    downloader=GithubDownloader,
    downloader_config=GithubDownloaderConfig,
    connection_config=GithubConnectionConfig,
)
