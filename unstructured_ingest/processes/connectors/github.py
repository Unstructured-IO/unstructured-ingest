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
from unstructured_ingest.errors_v2 import ProviderError, UserAuthError, UserError
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

    def wrap_github_exception(self, e: "GithubException") -> Exception:
        data = e.data
        status_code = e.status
        message = data.get("message")
        if status_code == 401:
            return UserAuthError(f"Unauthorized access to Github: {message}")
        if 400 <= status_code < 500:
            return UserError(message)
        if status_code > 500:
            return ProviderError(message)
        logger.debug(f"unhandled github error: {e}")
        return e

    def wrap_http_error(self, e: "HTTPError") -> Exception:
        status_code = e.response.status_code
        if status_code == 401:
            return UserAuthError(f"Unauthorized access to Github: {e.response.text}")
        if 400 <= status_code < 500:
            return UserError(e.response.text)
        if status_code > 500:
            return ProviderError(e.response.text)
        logger.debug(f"unhandled http error: {e}")
        return e

    @requires_dependencies(["requests"], extras="github")
    def wrap_error(self, e: Exception) -> Exception:
        from github.GithubException import GithubException
        from requests import HTTPError

        if isinstance(e, GithubException):
            return self.wrap_github_exception(e=e)
        if isinstance(e, HTTPError):
            return self.wrap_http_error(e=e)
        logger.debug(f"unhandled error: {e}")
        return e


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
    download_config: GithubDownloaderConfig
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
