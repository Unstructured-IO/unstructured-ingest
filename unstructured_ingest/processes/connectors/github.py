from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional
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

        401 and 403 both map to UserAuthError so the platform surfaces a
        reconnect-required state: 401 covers an invalid/expired/revoked access
        token (including the case where the platform's refresh token was revoked
        and init-oauth-refresh left a stale token in place), and 403 covers a
        missing OAuth scope or insufficient permission — both are resolved by the
        user reconnecting with adequate scopes.
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

        data = e.data if isinstance(e.data, dict) else {}
        message = data.get("message") or safe_error_summary(e)
        # PyGithub raises RateLimitExceededException with a 403 status for primary
        # rate limits, so classify by type before falling back to the status map.
        if isinstance(e, RateLimitExceededException):
            return RateLimitError(f"GitHub API rate limit exceeded: {message}")
        error = self._error_for_status(e.status, message)
        if error is not None:
            return error
        logger.debug(f"unhandled github error: {safe_error_summary(e)}")
        return e

    def wrap_http_error(self, e: "HTTPError") -> Exception:
        error = self._error_for_status(e.response.status_code, e.response.text)
        if error is not None:
            return error
        logger.debug(f"unhandled http error: {safe_error_summary(e)}")
        return UnstructuredIngestError(safe_error_summary(e))

    @requires_dependencies(["requests"], extras="github")
    def wrap_error(self, e: Exception) -> Exception:
        from github.GithubException import GithubException
        from requests import HTTPError

        if isinstance(e, GithubException):
            return self.wrap_github_exception(e=e)
        if isinstance(e, HTTPError):
            return self.wrap_http_error(e=e)
        logger.debug(f"unhandled error: {safe_error_summary(e)}")
        return UnstructuredIngestError(safe_error_summary(e))


@dataclass
class _GitFile:
    """A blob entry resolved to its full repo-relative path.

    The fast recursive-tree path and the truncation-fallback walk both produce
    these, so ``convert_element`` consumes a single uniform shape regardless of
    how the tree was listed.
    """

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


@dataclass
class GithubIndexer(Indexer):
    connection_config: GithubConnectionConfig
    index_config: GithubIndexerConfig = field(default_factory=GithubIndexerConfig)
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            self.connection_config.get_repo()
        except Exception as e:
            raise self.connection_config.wrap_error(e=e)

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
        git_tree = repo.get_git_tree(branch, recursive=recursive)
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
        tree = repo.get_git_tree(tree_ish, recursive=False)
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
                # Git blob SHA: a content hash already returned in the tree listing (no
                # extra API call). It changes iff the file content changes, which is
                # exactly what the platform's per-record incremental skip logic compares
                # on. (element.etag is the shared tree-response ETag — identical for every
                # file and bumped on any repo change — so it cannot drive per-file skip.)
                version=file.sha,
                record_locator={},
                # Git tree entries carry no per-file timestamp, and the only date the API
                # exposes here (the tree response's shared Last-Modified header) is not a
                # real per-file value, so date_modified is intentionally left unset. Not
                # needed for skip logic — version is the sole signal.
                date_processed=str(time()),
                filesize_bytes=file.size,
                permissions_data=[{"mode": file.mode}],
            ),
        )

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        # Fetch the repo (and thus default_branch) exactly once per run — convert_element
        # no longer re-derives them per file (previously 2 get_repo calls per file).
        repo = self.connection_config.get_repo()
        branch = self.get_branch(repo)
        for file in self.list_files(repo, branch):
            yield self.convert_element(file, branch)


class GithubDownloaderConfig(DownloaderConfig):
    pass


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
            self._repo = self.connection_config.get_repo()
        return self._repo

    @requires_dependencies(["github"], extras="github")
    def get_file(self, file_data: FileData) -> "ContentFile":
        from github.GithubException import UnknownObjectException

        path = file_data.source_identifiers.relative_path
        repo = self._get_repo()

        try:
            content_file = repo.get_contents(path)
        except UnknownObjectException as e:
            logger.error(f"File doesn't exists {self.connection_config.url}/{path}: {e}")
            raise UserError(f"File not found: {path}")
        return content_file

    @requires_dependencies(["requests"], extras="github")
    def get_contents(self, content_file: "ContentFile") -> bytes:
        import requests

        if content_file.decoded_content:
            return content_file.decoded_content
        download_url = content_file.download_url
        resp = requests.get(download_url)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise self.connection_config.wrap_error(e=e)
        return resp.content

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        content_file = self.get_file(file_data)
        contents = self.get_contents(content_file)
        download_path = self.get_download_path(file_data)
        # Repo files live under nested paths (e.g. src/com/.../Base64.as) and the
        # download dir itself may not exist yet, so create the full parent chain
        # before writing — nothing upstream does this for us.
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
