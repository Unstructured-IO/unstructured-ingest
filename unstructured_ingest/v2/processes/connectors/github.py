import fnmatch
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional
from urllib.parse import urlparse
from uuid import NAMESPACE_DNS, uuid5

import requests
from pydantic import Field, Secret, field_validator

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.errors import ProviderError, UserAuthError, UserError
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    Indexer,
    IndexerConfig,
    download_responses,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    SourceRegistryEntry,
)
from unstructured_ingest.v2.types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)

if TYPE_CHECKING:
    from github import ContentFile, GitTreeElement, Repository
    from github import Github as GithubClient

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
    file_glob: Optional[list[str]] = Field(
        default=None,
        description="file globs to limit which types of " "files are accepted",
        examples=["*.pdf", "*.html"],
    )


@dataclass
class GithubIndexer(Indexer):
    connection_config: GithubConnectionConfig
    index_config: GithubIndexerConfig = field(default_factory=GithubIndexerConfig)
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
        if self.index_config.file_glob:
            file_elements = self.filter_files(files=file_elements)
        return file_elements

    def filter_files(self, files: list["GitTreeElement"]) -> list["GitTreeElement"]:
        filtered_files = []
        for file in files:
            path = file.path
            for pattern in self.index_config.file_glob:
                if fnmatch.filter([path], pattern):
                    filtered_files.append(file)
        return filtered_files

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
                fullpath=(Path(self.get_branch()) / element.path).as_posix(),
                rel_path=element.path,
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

    @requires_dependencies(["github"], extras="github")
    def get_file(self, file_data: FileData) -> "ContentFile":
        from github.GithubException import UnknownObjectException

        path = file_data.source_identifiers.relative_path
        repo = self.connection_config.get_repo()

        try:
            content_file = repo.get_contents(path)
        except UnknownObjectException as e:
            logger.error(f"File doesn't exists {self.connection_config.url}/{path}: {e}")
            raise UserError(f"File not found: {path}")
        return content_file

    def wrap_error(self, e: requests.HTTPError) -> Exception:
        status_code = e.response.status_code
        if status_code == 401:
            return UserAuthError(f"Unauthorized access to Github: {e.response.text}")
        if 400 <= status_code < 500:
            return UserError(f"Client error: {e.response.text}")
        if status_code > 500:
            return ProviderError(f"Server error: {e.response.text}")
        logger.debug(f"unhandled http error: {e}")
        return e

    def get_contents(self, content_file: "ContentFile") -> bytes:
        if content_file.decoded_content:
            return content_file.decoded_content
        download_url = content_file.download_url
        resp = requests.get(download_url)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise self.wrap_error(e=e)
        return resp.content

    def get_output_path(self, file_data: FileData) -> Path:
        download_dir = self.downloader_config.download_dir
        output_path = download_dir / file_data.source_identifiers.fullpath
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        content_file = self.get_file(file_data)
        contents = self.get_contents(content_file)
        output_path = self.get_output_path(file_data)
        with output_path.open("wb") as f:
            f.write(contents)


github_source_entry = SourceRegistryEntry(
    indexer=GithubIndexer,
    indexer_config=GithubIndexerConfig,
    downloader=GithubDownloader,
    downloader_config=GithubDownloaderConfig,
    connection_config=GithubConnectionConfig,
)
