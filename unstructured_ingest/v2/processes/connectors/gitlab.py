from __future__ import annotations

from dataclasses import dataclass, field
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
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
    download_responses,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

CONNECTOR_TYPE = "gitlab"
if TYPE_CHECKING:
    from gitlab.v4.objects.projects import Project


class GitLabAccessConfig(AccessConfig):
    access_token: Optional[str] = Field(default=None)


class GitLabConnectionConfig(ConnectionConfig):
    url: str
    base_url: str = "https://gitlab.com"
    access_config: Secret[GitLabAccessConfig]
    git_branch: Optional[str] = Field(default=None)
    repo_path: str = field(init=False, repr=False, default=None)

    @root_validator(pre=True)
    def set_repo_path(cls, values):
        parsed_gh_url = urlparse(values.get("url"))
        # If a scheme or netloc are provided, use the parsed base url
        if parsed_gh_url.scheme or parsed_gh_url.netloc:
            values["base_url"] = f"{parsed_gh_url.scheme}://{parsed_gh_url.netloc}"
        values["repo_path"] = parsed_gh_url.path
        while values["repo_path"].startswith("/"):
            values["repo_path"] = values["repo_path"][1:]

        return values

    @SourceConnectionError.wrap
    @requires_dependencies(["gitlab"], extras="gitlab")
    def get_project(self) -> "Project":
        from gitlab import Gitlab

        logger.info(f"Accessing Base URL: '{self.base_url}'", extra={"base_url": self.base_url})

        gitlab = Gitlab(
            self.base_url, private_token=self.access_config.get_secret_value().access_token
        )
        logger.info(f"Accessing Project: '{self.repo_path}'", extra={"repo_path": self.repo_path})

        project = gitlab.projects.get(self.repo_path)
        logger.info(
            f"Successfully accessed project '{self.repo_path}'", extra={"repo_path": self.repo_path}
        )
        return project


class GitLabIndexerConfig(IndexerConfig):
    pass


@dataclass
class GitLabIndexer(Indexer):
    connection_config: GitLabConnectionConfig
    index_config: GitLabIndexerConfig
    registry_name: str = "gitlab"

    @requires_dependencies(["gitlab"], extras="gitlab")
    def precheck(self) -> None:
        from gitlab import Gitlab
        from gitlab.exceptions import GitlabError

        try:
            gitlab = Gitlab(
                self.connection_config.base_url,
                private_token=self.connection_config.access_config.get_secret_value().access_token,
            )
            if self.connection_config.access_config.get_secret_value().access_token is not None:
                gitlab.auth()
            else:
                gitlab.projects.get(self.connection_config.repo_path)
        except GitlabError as gitlab_error:
            logger.error(f"Failed to validate connection: {gitlab_error}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {gitlab_error}")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        project = self.connection_config.get_project()
        ref = self.connection_config.git_branch or project.default_branch
        git_tree = project.repository_tree(
            ref=ref,
            recursive=True,
            iterator=True,
            all=True,
        )
        for element in git_tree:
            rel_path = element["path"].replace(self.connection_config.repo_path, "").lstrip("/")
            if element["type"] == "blob":
                record_locator = {
                    "file_path": element["path"],
                    "file_name": element["name"],
                    "file_type": element["type"],
                    "repo_path": self.connection_config.repo_path,
                }
                if self.connection_config.git_branch is not None:
                    record_locator["branch"] = self.connection_config.git_branch

                yield FileData(
                    identifier=element["id"],
                    connector_type=CONNECTOR_TYPE,
                    source_identifiers=SourceIdentifiers(
                        fullpath=element["path"],
                        filename=element["path"].split("/")[-1],
                        rel_path=rel_path,
                    ),
                    metadata=FileDataSourceMetadata(
                        url=element["id"],
                        record_locator=record_locator,
                        permissions_data=[{"mode": element["mode"]}],
                    ),
                    additional_metadata={},
                )


class GitLabDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class GitLabDownloader(Downloader):
    connection_config: GitLabConnectionConfig
    download_config: GitLabDownloaderConfig

    @SourceConnectionNetworkError.wrap
    @requires_dependencies(["gitlab"], extras="gitlab")
    def _fetch_content(self, path):
        from gitlab.exceptions import GitlabHttpError

        try:
            project = self.connection_config.get_project()
            content_file = project.files.get(
                path,
                ref=self.connection_config.git_branch or project.default_branch,
            )
        except GitlabHttpError as e:
            if e.response_code == 404:
                logger.error(f"File doesn't exists {self.connection_config.url}/{self.path}")
                raise e
            raise
        return content_file

    def _fetch_and_write(self, path, download_path) -> None:
        content_file = self._fetch_content(path)
        if content_file is None:
            raise ValueError(
                f"Failed to retrieve file from repo "
                f"{self.connection_config.url}/{self.path}. Check logs.",
            )
        contents = content_file.decode()
        with open(download_path, "wb") as f:
            f.write(contents)

    @SourceConnectionError.wrap
    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)

        path = file_data.source_identifiers.fullpath
        self._fetch_and_write(path, download_path)

        return DownloadResponse(file_data=file_data, path=download_path)


gitlab_source_entry = SourceRegistryEntry(
    connection_config=GitLabConnectionConfig,
    indexer_config=GitLabIndexerConfig,
    indexer=GitLabIndexer,
    downloader_config=GitLabDownloaderConfig,
    downloader=GitLabDownloader,
)
