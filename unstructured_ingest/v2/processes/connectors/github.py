from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional
from urllib.parse import urlparse
from uuid import NAMESPACE_DNS, uuid5

from pydantic import Field, Secret, field_validator

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    Indexer,
    IndexerConfig,
)
from unstructured_ingest.v2.processes.connector_registry import (
    SourceRegistryEntry,
)
from unstructured_ingest.v2.types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)

if TYPE_CHECKING:
    from github import Github as GithubClient
    from github import GitTreeElement, Repository

CONNECTOR_TYPE = "github"


class GithubAccessConfig(AccessConfig):
    access_token: str = Field(description="Github acess token")


class GithubConnectionConfig(ConnectionConfig):
    access_config: Secret[GithubAccessConfig]
    url: str = Field(description="Github url or repository owner/name pair")

    @field_validator("url", mode="after")
    def conform_url(cls, value: str):
        parsed_url = urlparse(value)
        return parsed_url.path

    def get_full_url(self):
        return f"https://github.com/{self.url}"

    @requires_dependencies(["github"], extras="github")
    def get_client(self) -> "GithubClient":
        from github import Github as GithubClient

        return GithubClient(login_or_token=self.access_config.get_secret_value().access_token)

    def get_repo(self) -> "Repository":
        client = self.get_client()
        return client.get_repo(self.url)


class GithubIndexerConfig(IndexerConfig):
    branch: Optional[str] = Field(
        description="Branch to index, use the default if one isn't provided", default=None
    )
    recursive: bool = Field(
        description="Recursively index all files in the repository", default=True
    )


@dataclass
class GithubIndexer(Indexer):
    index_config: GithubIndexerConfig
    connection_config: GithubConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def get_branch(self) -> str:
        repo = self.connection_config.get_repo()
        sha = self.index_config.branch or repo.default_branch
        return sha

    def list_files(self) -> list["GitTreeElement"]:
        repo = self.connection_config.get_repo()
        sha = self.index_config.branch or repo.default_branch
        git_tree = repo.get_git_tree(sha, recursive=self.index_config.recursive)
        file_elements = [
            element for element in git_tree.tree if element.size is not None and element.size > 0
        ]
        return file_elements

    def convert_element(self, element: "GitTreeElement") -> FileData:
        full_path = (
            f"{self.connection_config.get_full_url()}/blob/{self.get_branch()}/{element.path}"
        )

        return FileData(
            identifier=str(uuid5(NAMESPACE_DNS, full_path)),
            connector_type=self.connector_type,
            display_name=full_path,
            source_identifiers=SourceIdentifiers(
                filename=Path(element.path).name,
                fullpath=(Path(self.get_branch()) / Path(element.path).name).as_posix(),
            ),
            metadata=FileDataSourceMetadata(
                url=element.url,
                version=element.etag,
                record_locator={},
                date_modified=str(element.last_modified_datetime.timestamp()),
                date_processed=str(time()),
                filesize_bytes=element.size,
                permissions_data=[{"mode": element.mode}],
            ),
        )

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        for element in self.list_files():
            yield self.convert_element(element=element)


class GithubDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class GithubDownloader(Downloader):
    downloader_config: GithubDownloaderConfig
    connection_config: GithubConnectionConfig
    connector_type: str = CONNECTOR_TYPE


github_source_entry = SourceRegistryEntry(
    indexer=GithubIndexer,
    indexer_config=GithubIndexerConfig,
    downloader=GithubDownloader,
    downloader_config=GithubDownloaderConfig,
    connection_config=GithubConnectionConfig,
)
