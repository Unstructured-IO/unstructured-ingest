from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import DownloadResponse, FileData, FileDataSourceMetadata
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.fsspec.fsspec import (
    FsspecAccessConfig,
    FsspecConnectionConfig,
    FsspecDownloader,
    FsspecDownloaderConfig,
    FsspecIndexer,
    FsspecIndexerConfig,
    FsspecUploader,
    FsspecUploaderConfig,
)

CONNECTOR_TYPE = "dropbox"


class DropboxIndexerConfig(FsspecIndexerConfig):
    pass


class DropboxAccessConfig(FsspecAccessConfig):
    token: Optional[str] = Field(default=None, description="Dropbox access token.")


class DropboxConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = field(default_factory=lambda: ["dropbox"], init=False)
    access_config: Secret[DropboxAccessConfig] = Field(
        default=DropboxAccessConfig(), validate_default=True
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)


@dataclass
class DropboxIndexer(FsspecIndexer):
    connection_config: DropboxConnectionConfig
    index_config: DropboxIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def get_path(self, file_data: dict) -> str:
        return file_data["name"]

    def get_metadata(self, file_data: dict) -> FileDataSourceMetadata:
        path = file_data["name"].lstrip("/")
        date_created = None
        date_modified = None
        server_modified = file_data.get("server_modified")
        client_modified = file_data.get("client_modified")
        if server_modified and client_modified and server_modified > client_modified:
            date_created = str(client_modified.timestamp())
            date_modified = str(server_modified.timestamp())
        elif server_modified and client_modified and server_modified < client_modified:
            date_created = str(server_modified.timestamp())
            date_modified = str(client_modified.timestamp())

        file_size = file_data.get("size") if "size" in file_data else None

        version = file_data.get("content_hash")
        record_locator = {
            "protocol": self.index_config.protocol,
            "remote_file_path": self.index_config.remote_url,
            "file_id": file_data.get("id"),
        }
        return FileDataSourceMetadata(
            date_created=date_created,
            date_modified=date_modified,
            date_processed=str(time()),
            version=version,
            url=f"{self.index_config.protocol}://{path}",
            record_locator=record_locator,
            filesize_bytes=file_size,
        )

    @requires_dependencies(["dropboxdrivefs", "fsspec"], extras="dropbox")
    def __post_init__(self):
        # dropbox expects the path to start with a /
        if not self.index_config.path_without_protocol.startswith("/"):
            self.index_config.path_without_protocol = "/" + self.index_config.path_without_protocol

    @requires_dependencies(["dropboxdrivefs", "fsspec"], extras="dropbox")
    def precheck(self) -> None:
        super().precheck()

    @requires_dependencies(["dropboxdrivefs", "fsspec"], extras="dropbox")
    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        return super().run(**kwargs)


class DropboxDownloaderConfig(FsspecDownloaderConfig):
    pass


@dataclass
class DropboxDownloader(FsspecDownloader):
    protocol: str = "dropbox"
    connection_config: DropboxConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[DropboxDownloaderConfig] = field(
        default_factory=DropboxDownloaderConfig
    )

    @requires_dependencies(["dropboxdrivefs", "fsspec"], extras="dropbox")
    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return super().run(file_data=file_data, **kwargs)

    @requires_dependencies(["dropboxdrivefs", "fsspec"], extras="dropbox")
    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return await super().run_async(file_data=file_data, **kwargs)


class DropboxUploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class DropboxUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: DropboxConnectionConfig
    upload_config: DropboxUploaderConfig = field(default=None)

    @requires_dependencies(["dropboxdrivefs", "fsspec"], extras="dropbox")
    def __post_init__(self):
        super().__post_init__()

    @requires_dependencies(["dropboxdrivefs", "fsspec"], extras="dropbox")
    def precheck(self) -> None:
        super().precheck()

    @requires_dependencies(["dropboxdrivefs", "fsspec"], extras="dropbox")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        return super().run(path=path, file_data=file_data, **kwargs)

    @requires_dependencies(["dropboxdrivefs", "fsspec"], extras="dropbox")
    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        return await super().run_async(path=path, file_data=file_data, **kwargs)


dropbox_source_entry = SourceRegistryEntry(
    indexer=DropboxIndexer,
    indexer_config=DropboxIndexerConfig,
    downloader=DropboxDownloader,
    downloader_config=DropboxDownloaderConfig,
    connection_config=DropboxConnectionConfig,
)

dropbox_destination_entry = DestinationRegistryEntry(
    uploader=DropboxUploader,
    uploader_config=DropboxUploaderConfig,
    connection_config=DropboxConnectionConfig,
)
