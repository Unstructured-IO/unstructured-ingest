from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional
from urllib.parse import urlparse

from pydantic import Field, Secret, root_validator

from unstructured_ingest.error import SourceConnectionError, SourceConnectionNetworkError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
    download_responses,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

CONNECTOR_TYPE = "github"
if TYPE_CHECKING:
    from github.Repository import Repository


class GitHubAccessConfig(AccessConfig):
    git_access_token: Optional[str] = Field(
        default=None,
    )


class GitHubConnectionConfig(ConnectionConfig):
    url: str
    access_config: Secret[GitHubAccessConfig]
    branch: Optional[str] = Field(default=None, overload_name="git_branch")
    repo_path: str = field(init=False, repr=False, default=None)

    @root_validator(pre=True)
    def set_repo_path(cls, values):
        # Parse the URL
        url = values.get("url")
        if url:
            parsed_gh_url = urlparse(url)
            path_fragments = [fragment for fragment in parsed_gh_url.path.split("/") if fragment]

            # Validate the URL and construct the repo_path
            if (
                (parsed_gh_url.scheme and parsed_gh_url.scheme != "https")
                or (parsed_gh_url.netloc and parsed_gh_url.netloc != "github.com")
                or len(path_fragments) != 2
            ):
                raise ValueError(
                    'Please provide a valid URL, e.g. "https://github.com/owner/repo" or '
                    '"owner/repo".'
                )

            # Set the repo_path based on URL fragments
            values["repo_path"] = "/".join(path_fragments)
        return values

    @SourceConnectionError.wrap
    @requires_dependencies(["github"], extras="github")
    def get_repo(self) -> "Repository":
        from github import Github

        github = Github(self.access_config.get_secret_value().git_access_token)
        return github.get_repo(self.repo_path)


class GitHubIndexerConfig(IndexerConfig):
    pass


@dataclass
class GitHubIndexer(Indexer):
    connection_config: GitHubConnectionConfig
    index_config: GitHubIndexerConfig

    def precheck(self) -> None:
        from github import Auth, Consts
        from github.GithubRetry import GithubRetry
        from github.Requester import Requester

        auth = Auth.Token(self.connection_config.access_config.get_secret_value().git_access_token)

        try:
            requester = Requester(
                auth=auth,
                base_url=Consts.DEFAULT_BASE_URL,
                timeout=Consts.DEFAULT_TIMEOUT,
                user_agent=Consts.DEFAULT_USER_AGENT,
                per_page=Consts.DEFAULT_PER_PAGE,
                verify=True,
                retry=GithubRetry(),
                pool_size=None,
            )
            url = f"{Consts.DEFAULT_BASE_URL}/repos/{self.connection_config.repo_path}"
            logger.debug(f"Precheck Request: '{url}'")

            headers, _ = requester.requestJsonAndCheck("HEAD", url)
            logger.debug(f"Headers from HEAD request: {headers}")
        except Exception as e:
            logger.error(f"Failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        repo = self.connection_config.get_repo()

        # Load the Git tree with all files, and then create Ingest docs
        # for all blobs, i.e. all files, ignoring directories
        sha = self.connection_config.branch or repo.default_branch
        git_tree = repo.get_git_tree(sha, recursive=True)

        for element in git_tree.tree:
            rel_path = element.path.replace(self.connection_config.repo_path, "").lstrip("/")
            if element.type == "blob":
                record_locator = {
                    "repo_path": self.connection_config.repo_path,
                    "file_path": element.path,
                }
                if self.connection_config.branch is not None:
                    record_locator["branch"] = self.connection_config.branch

                date_modified = datetime.strptime(
                    element._headers["last-modified"],
                    "%a, %d %b %Y %H:%M:%S %Z",
                ).isoformat()

                date_created = datetime.strptime(
                    element._headers["date"],
                    "%a, %d %b %Y %H:%M:%S %Z",
                ).isoformat()

                yield FileData(
                    identifier=element.sha,
                    connector_type=CONNECTOR_TYPE,
                    source_identifiers=SourceIdentifiers(
                        fullpath=element.path,
                        filename=element.path.split("/")[-1],
                        rel_path=rel_path,
                    ),
                    metadata=FileDataSourceMetadata(
                        url=element.url,
                        version=element.etag,
                        record_locator=record_locator,
                        date_modified=date_modified,
                        date_created=date_created,
                        date_processed=str(time()),
                    ),
                    additional_metadata={
                        "content-type": element._headers.get("content-type", ""),
                        "mode": element._rawData.get("mode", ""),
                        "type": element._rawData.get("type", ""),
                        "size": element._rawData.get("size", ""),
                    },
                )


class GitHubDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class GitHubDownloader(Downloader):
    connection_config: GitHubConnectionConfig
    download_config: GitHubDownloaderConfig

    @requires_dependencies(["github"], extras="github")
    def _fetch_file(self, path):
        from github.GithubException import UnknownObjectException

        try:
            content_file = self.connection_config.get_repo().get_contents(path)
        except UnknownObjectException:
            logger.error(f"File doesn't exists {self.connection_config.url}/{path}")
            raise UnknownObjectException

        return content_file

    @SourceConnectionNetworkError.wrap
    @requires_dependencies(["requests"], extras="github")
    def _fetch_content(self, content_file):
        import requests
        from github.GithubException import UnknownObjectException

        contents = b""
        if (
            not content_file.content  # type: ignore
            and content_file.encoding == "none"  # type: ignore
            and content_file.size  # type: ignore
        ):
            logger.info("File too large for the GitHub API, using direct download link instead.")
            response = requests.get(content_file.download_url)  # type: ignore
            response.raise_for_status()
            if response.status_code != 200:
                logger.info("Direct download link has failed... Skipping this file.")
                raise UnknownObjectException
            else:
                contents = response.content
        else:
            contents = content_file.decoded_content  # type: ignore
        return contents

    def _fetch_and_write(self, path) -> None:
        content_file = self._fetch_file(path)
        contents = self._fetch_content(content_file)
        if contents is None:
            raise ValueError(
                f"Failed to retrieve file from repo "
                f"{self.connector_config.url}/{self.path}. Check logs",
            )
        with open(path, "wb") as f:
            f.write(contents)

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)

        path = file_data.source_identifiers.fullpath
        self._fetch_and_write(path)

        return self.generate_download_response(file_data=file_data, download_path=download_path)


github_source_entry = SourceRegistryEntry(
    connection_config=GitHubConnectionConfig,
    indexer_config=GitHubIndexerConfig,
    indexer=GitHubIndexer,
    downloader_config=GitHubDownloaderConfig,
    downloader=GitHubDownloader,
)
