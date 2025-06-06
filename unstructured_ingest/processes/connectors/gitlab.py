from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional
from urllib.parse import urlparse

from pydantic import Field, Secret, model_validator

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import SourceRegistryEntry
from unstructured_ingest.utils.dep_check import requires_dependencies

CONNECTOR_TYPE = "gitlab"
if TYPE_CHECKING:
    from gitlab import Gitlab
    from gitlab.v4.objects.projects import Project


class GitLabAccessConfig(AccessConfig):
    access_token: Optional[str] = Field(
        default=None,
        description="Optional personal access token for authenticating with the GitLab API.",
    )


class GitLabConnectionConfig(ConnectionConfig):
    access_config: Secret[GitLabAccessConfig] = Field(
        default_factory=GitLabAccessConfig,
        validate_default=True,
        description="Secret configuration for accessing the GitLab API by authentication token.",
    )
    url: str = Field(description="The full URL to the GitLab project or repository.")
    base_url: str = Field(
        default="https://gitlab.com",
        description="The base URL for the GitLab instance (default is GitLab's public domain).",
    )
    repo_path: str = Field(
        default=None,
        init=False,
        repr=False,
        description="The normalized path extracted from the repository URL.",
    )

    @model_validator(mode="after")
    def set_repo_path(self):
        """
        Parses the provided GitLab URL to extract the `base_url` and `repo_path`,
        ensuring both are properly formatted for use.

        If the URL contains a scheme (e.g., 'https') and a network location (e.g., 'gitlab.com'),
        the `base_url` is set accordingly. The repository path is extracted and normalized
        by removing any leading slashes.

        Notes:
            - If the URL contains both a scheme and network location, the `base_url` is
              extracted directly from the URL.
            - The `repo_path` is adjusted to remove any leading slashes.
            - This method assumes that the URL follows GitLab's structure
              (e.g., 'https://gitlab.com/owner/repo').
        """
        parsed_gh_url = urlparse(self.url)

        if parsed_gh_url.scheme and parsed_gh_url.netloc:
            self.base_url = f"{parsed_gh_url.scheme}://{parsed_gh_url.netloc}"
        self.repo_path = parsed_gh_url.path.lstrip("/")

        return self

    @SourceConnectionError.wrap
    @requires_dependencies(["gitlab"], extras="gitlab")
    @contextmanager
    def get_client(self) -> Generator["Gitlab", None, None]:
        from gitlab import Gitlab

        logger.info(f"Connection to GitLab: {self.base_url!r}")
        with Gitlab(
            self.base_url, private_token=self.access_config.get_secret_value().access_token
        ) as client:
            yield client

    @contextmanager
    def get_project(self) -> Generator["Project", None, None]:
        """Retrieves the specified GitLab project using the configured base URL and access token.

        Returns:
            Project: A GitLab `Project` object representing the specified repository.

        Raises:
            SourceConnectionError: If the GitLab API connection fails.
            gitlab.exceptions.GitlabGetError: If the project is not found.
        """
        with self.get_client() as client:
            logger.info(f"Accessing Project: '{self.repo_path}'")
            project = client.projects.get(self.repo_path)

            logger.info(f"Successfully accessed project '{self.repo_path}'")
            yield project


class GitLabIndexerConfig(IndexerConfig):
    path: Path = Field(
        default="/", description=("Path to the location in the repository that will be processed.")
    )
    recursive: bool = Field(
        default=True,
        description=(
            "Flag to control recursive operations when indexing. "
            "If True, the indexer will traverse directories recursively."
        ),
    )
    git_branch: Optional[str] = Field(
        default=None,
        description="The name of the branch to interact with.",
    )


@dataclass
class GitLabIndexer(Indexer):
    connection_config: GitLabConnectionConfig
    index_config: GitLabIndexerConfig

    def precheck(self) -> None:
        """Validates the connection to the GitLab instance by authenticating or
        accessing the project.

        This method ensures that the GitLab credentials and configuration are correct by
        either authenticating or attempting to fetch the specified project.

        Raises:
            SourceConnectionError: If the connection or authentication with GitLab fails.
        """

        try:
            with self.connection_config.get_client() as client:
                if self.connection_config.access_config.get_secret_value().access_token is not None:
                    client.auth()
                else:
                    client.projects.get(self.connection_config.repo_path)

        except Exception as e:
            logger.error(f"Failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        """Iterates over the GitLab repository tree and yields file metadata as `FileData` objects.

        This method fetches the repository tree for the specified branch and iterates
        over its contents. For each file (blob), it generates a `FileData` object containing
        the file's metadata, path, and permissions.

        Args:
            **kwargs (Any): Additional keyword arguments (if required).

        Yields:
            FileData: A generator that yields `FileData` objects representing each file (blob)
            in the repository.
        """
        with self.connection_config.get_project() as project:
            ref = self.index_config.git_branch or project.default_branch

            files = project.repository_tree(
                path=str(self.index_config.path),
                ref=ref,
                recursive=self.index_config.recursive,
                iterator=True,
                all=True,
            )

        for file in files:
            relative_path = str(Path(file["path"]).relative_to(self.index_config.path))
            if file["type"] == "blob":
                record_locator = {
                    "file_path": file["path"],
                    "ref": ref,
                }
                source_identifiers = SourceIdentifiers(
                    fullpath=file["path"],
                    filename=Path(file["path"]).name,
                    rel_path=relative_path,
                )
                yield FileData(
                    identifier=file["id"],
                    connector_type=CONNECTOR_TYPE,
                    source_identifiers=source_identifiers,
                    metadata=FileDataSourceMetadata(
                        url=file["id"],
                        record_locator=record_locator,
                        permissions_data=[{"mode": file["mode"]}],
                    ),
                    additional_metadata={},
                    display_name=source_identifiers.fullpath,
                )


class GitLabDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class GitLabDownloader(Downloader):
    connection_config: GitLabConnectionConfig
    download_config: GitLabDownloaderConfig

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        """Downloads a file from the repository and returns a `DownloadResponse`.

        Args:
            file_data (FileData): Metadata about the file to be downloaded.
            **kwargs (Any): Additional arguments (if required).

        Returns:
            DownloadResponse: A response object containing the download details.
        """
        download_path = self.get_download_path(file_data=file_data)
        if download_path is None:
            logger.error(
                "Generated download path is None, source_identifiers might be missingfrom FileData."
            )
            raise ValueError("Generated invalid download path.")

        self._download_file(file_data, download_path)
        return self.generate_download_response(file_data=file_data, download_path=download_path)

    def _download_file(self, file_data: FileData, download_path: Path) -> None:
        # NOTE: Indexer should supply the record locator in metadata
        if (
            file_data.metadata.record_locator is None
            or "ref" not in file_data.metadata.record_locator
            or "file_path" not in file_data.metadata.record_locator
        ):
            logger.error(
                f"Invalid record locator in metadata: {file_data.metadata.record_locator}."
                "Keys 'ref' and 'path' must be present."
            )
            raise ValueError("Invalid record locator.")

        ref = file_data.metadata.record_locator["ref"]
        path = file_data.metadata.record_locator["file_path"]
        download_path.parent.mkdir(exist_ok=True, parents=True)

        with self.connection_config.get_project() as project:
            project_file = project.files.get(file_path=path, ref=ref)
            with open(download_path, "wb") as file:
                file.write(project_file.decode())


gitlab_source_entry = SourceRegistryEntry(
    connection_config=GitLabConnectionConfig,
    indexer_config=GitLabIndexerConfig,
    indexer=GitLabIndexer,
    downloader_config=GitLabDownloaderConfig,
    downloader=GitLabDownloader,
)
