from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional
from urllib.parse import urlparse

from pydantic import Field, Secret, model_validator

from unstructured_ingest.error import SourceConnectionError, SourceConnectionNetworkError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

CONNECTOR_TYPE = "github"
if TYPE_CHECKING:
    from github.ContentFile import ContentFile
    from github.Repository import Repository


class GitHubAccessConfig(AccessConfig):
    git_access_token: Optional[str] = Field(
        default=None,
        description="Optional personal access token for authenticating with the GitHub API.",
    )


class GitHubConnectionConfig(ConnectionConfig):
    url: str = Field(
        description=(
            "The full URL to the GitHub project or repository, used to determine"
            "the base URL and repo path."
        )
    )

    access_config: Secret[GitHubAccessConfig] = Field(
        default=GitHubAccessConfig(),
        validate_default=True,
        description="Secret configuration for accessing the GitHub API by authentication tokens.",
    )

    branch: Optional[str] = Field(
        default=None,
        overload_name="git_branch",
        description=(
            "The branch to interact with. If not provided, the default branch for the"
            " repository is used."
        ),
    )

    repo_path: str = Field(
        default=None,
        init=False,
        repr=False,
        description="The normalized repository path extracted from the GitHub URL.",
    )

    @model_validator(mode="before")
    def set_repo_path(cls, values: dict) -> dict:
        """Parses the provided GitHub URL and sets the `repo_path` value.

        This method ensures the provided URL is valid and properly formatted, extracting
        the owner and repository name as the `repo_path`. If the URL is invalid, it raises
        a `ValueError`.

        Args:
            values (dict): A dictionary of field values passed to the model.

        Returns:
            dict: The updated dictionary of values with the `repo_path` field set.

        Raises:
            ValueError: If the URL is not properly formatted or doesn't match the
            expected GitHub structure.
        """
        url = values.get("url")
        if url:
            parsed_gh_url = urlparse(url)
            path_fragments = [fragment for fragment in parsed_gh_url.path.split("/") if fragment]

            if (
                (parsed_gh_url.scheme and parsed_gh_url.scheme != "https")
                or (parsed_gh_url.netloc and parsed_gh_url.netloc != "github.com")
                or len(path_fragments) != 2
            ):
                raise ValueError(
                    'Please provide a valid URL, e.g. "https://github.com/owner/repo" or '
                    '"owner/repo".'
                )

            values["repo_path"] = "/".join(path_fragments)
        return values

    @SourceConnectionError.wrap
    @requires_dependencies(["github"], extras="github")
    def get_repo(self) -> "Repository":
        from github import Github

        github = Github(self.access_config.get_secret_value().git_access_token)
        return github.get_repo(self.repo_path)


class GitHubIndexerConfig(IndexerConfig):
    recursive: bool = Field(
        default=False,
        description=(
            "Flag to control recursive operations when indexing. "
            "If True, the indexer will traverse directories recursively."
        ),
    )


@dataclass
class GitHubIndexer(Indexer):
    connection_config: GitHubConnectionConfig
    index_config: GitHubIndexerConfig

    def precheck(self) -> None:
        """Performs a precheck to validate the connection to the GitHub repository.

        This method sends a `HEAD` request to the GitHub API to ensure the repository
        is accessible and properly configured. It uses the GitHub `Requester` class
        with retry support and authentication via an access token.

        Raises:
            SourceConnectionError: If the connection validation fails.
        """
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
            logger.debug(f"Precheck Request: {url!r}")

            headers, _ = requester.requestJsonAndCheck("HEAD", url)
            logger.debug(f"Headers from HEAD request: {headers}")
        except Exception as e:
            logger.error(f"Failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        """Iterates over the GitHub repository tree, yielding `FileData` objects for all
        files (blobs).

        This method retrieves the entire repository tree for the specified branch or
        the default branch.
        For each file (blob), it extracts relevant metadata and yields a `FileData` object.

        Args:
            **kwargs (Any): Additional optional arguments.

        Yields:
            FileData: An object containing metadata and identifiers for each file in the repository.
        """
        repo = self.connection_config.get_repo()
        sha = self.connection_config.branch or repo.default_branch
        logger.info(f"Starting to look for blob files on GitHub in branch: {sha!r}")

        git_tree = repo.get_git_tree(sha, recursive=self.index_config.recursive)

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

                additional_metadata = {}
                for metadata in ["content-type", "mode", "type", "size"]:
                    if metadata in element._headers:
                        additional_metadata[metadata] = element._headers[metadata]

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
                    additional_metadata=additional_metadata,
                )


class GitHubDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class GitHubDownloader(Downloader):
    connection_config: GitHubConnectionConfig
    download_config: GitHubDownloaderConfig

    def is_async(self) -> bool:
        return True

    @requires_dependencies(["github"], extras="github")
    def _fetch_file(self, path: str) -> ContentFile:
        """Fetches a file from the GitHub repository using the GitHub API.

        Args:
            path (str): The path to the file in the repository.

        Returns:
            ContentFile: An object containing the file content.
        """
        try:
            logger.info(f"Fetching file from path: {path!r}")
            content_file = self.connection_config.get_repo().get_contents(path)
        except Exception as e:
            logger.error(f"Failed to download {path}: {e}")
            raise e

        return content_file

    @SourceConnectionNetworkError.wrap
    @requires_dependencies(["httpx", "github"], extras="github")
    async def _fetch_content(self, content_file: ContentFile) -> bytes:
        """Asynchronously retrieves the content of a file, handling large files via direct download.

        Args:
            content_file (ContentFile): The file object from the GitHub API.

        Returns:
            bytes: The content of the file as bytes.
        """
        import httpx

        contents = b""
        async with httpx.AsyncClient() as client:
            if not content_file.content and content_file.encoding == "none" and content_file.size:
                logger.info(
                    "File too large for the GitHub API, using direct download link instead."
                )
                try:
                    response = await client.get(content_file.download_url, timeout=10.0)
                    response.raise_for_status()
                except Exception as e:
                    logger.error(f"Failed to download: {e}")
                    raise e

                contents = response.content
            else:
                contents = content_file.decoded_content
        return contents

    async def _fetch_and_write(self, path: str, download_path: Path) -> None:
        """Fetches a file from GitHub and writes its content to the specified local path.

        Args:
            path (str): The path to the file in the repository.
            download_path (Path): The local path to save the downloaded file.

        Raises:
            ValueError: If the file content could not be retrieved.
        """
        content_file = self._fetch_file(path)
        contents = await self._fetch_content(content_file)
        if contents is None:
            raise ValueError(
                f"Failed to retrieve file from repo "
                f"{self.connection_config.url}/{path}. Check logs",
            )

        logger.info(f"Writing download file to path: {download_path!r}")
        with download_path.open("wb") as f:
            f.write(contents)

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        # Synchronous run is not implemented
        raise NotImplementedError()

    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        """Asynchronously downloads a file from the GitHub repository and returns a
        `DownloadResponse`.

        Args:
            file_data (FileData): Metadata about the file to be downloaded.
            **kwargs (Any): Additional optional arguments.

        Returns:
            DownloadResponse: A response containing the details of the download.
        """
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)

        path = file_data.source_identifiers.fullpath
        await self._fetch_and_write(path, download_path)

        return self.generate_download_response(file_data=file_data, download_path=download_path)


github_source_entry = SourceRegistryEntry(
    connection_config=GitHubConnectionConfig,
    indexer_config=GitHubIndexerConfig,
    indexer=GitHubIndexer,
    downloader_config=GitHubDownloaderConfig,
    downloader=GitHubDownloader,
)
