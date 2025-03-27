from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional
from urllib.parse import urlparse

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData, FileDataSourceMetadata
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.processes.connectors.fsspec.fsspec import (
    FsspecAccessConfig,
    FsspecConnectionConfig,
    FsspecDownloader,
    FsspecDownloaderConfig,
    FsspecIndexer,
    FsspecIndexerConfig,
    FsspecUploader,
    FsspecUploaderConfig,
)
from unstructured_ingest.processes.utils.blob_storage import (
    BlobStoreUploadStager,
    BlobStoreUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from fsspec.implementations.sftp import SFTPFileSystem

CONNECTOR_TYPE = "sftp"


class SftpIndexerConfig(FsspecIndexerConfig):
    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        _, ext = os.path.splitext(self.remote_url)
        parsed_url = urlparse(self.remote_url)
        if ext:
            self.path_without_protocol = Path(parsed_url.path).parent.as_posix().lstrip("/")
        else:
            self.path_without_protocol = parsed_url.path.lstrip("/")


class SftpAccessConfig(FsspecAccessConfig):
    password: str = Field(description="Password for sftp connection")


class SftpConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = Field(default_factory=lambda: ["sftp"], init=False)
    access_config: Secret[SftpAccessConfig]
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    username: str = Field(description="Username for sftp connection")
    host: Optional[str] = Field(default=None, description="Hostname for sftp connection")
    port: int = Field(default=22, description="Port for sftp connection")
    look_for_keys: bool = Field(
        default=False, description="Whether to search for private key files in ~/.ssh/"
    )
    allow_agent: bool = Field(default=False, description="Whether to connect to the SSH agent.")

    def get_access_config(self) -> dict[str, Any]:
        access_config = {
            "username": self.username,
            "host": self.host,
            "port": self.port,
            "look_for_keys": self.look_for_keys,
            "allow_agent": self.allow_agent,
            "password": self.access_config.get_secret_value().password,
        }
        return access_config

    @contextmanager
    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    def get_client(self, protocol: str) -> Generator["SFTPFileSystem", None, None]:
        # The paramiko.SSHClient() client that's opened by the SFTPFileSystem
        # never gets closed so explicitly adding that as part of this context manager
        from fsspec import get_filesystem_class

        client: SFTPFileSystem = get_filesystem_class(protocol)(
            **self.get_access_config(),
        )
        yield client
        client.client.close()


@dataclass
class SftpIndexer(FsspecIndexer):
    connection_config: SftpConnectionConfig
    index_config: SftpIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def __post_init__(self):
        parsed_url = urlparse(self.index_config.remote_url)
        self.connection_config.host = parsed_url.hostname or self.connection_config.host
        self.connection_config.port = parsed_url.port or self.connection_config.port

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        for file in super().run(**kwargs):
            new_identifier = (
                f"sftp://"
                f"{self.connection_config.host}:"
                f"{self.connection_config.port}/"
                f"{file.identifier}"
            )
            file.identifier = new_identifier
            yield file

    def get_metadata(self, file_info: dict) -> FileDataSourceMetadata:
        path = file_info["name"]
        date_created = str(file_info.get("time").timestamp()) if "time" in file_info else None
        date_modified = str(file_info.get("mtime").timestamp()) if "mtime" in file_info else None

        file_size = file_info.get("size") if "size" in file_info else None

        record_locator = {
            "protocol": self.index_config.protocol,
            "remote_file_path": self.index_config.remote_url,
        }
        return FileDataSourceMetadata(
            date_created=date_created,
            date_modified=date_modified,
            date_processed=str(time()),
            url=f"{self.index_config.protocol}://{path}",
            record_locator=record_locator,
            filesize_bytes=file_size,
        )


class SftpDownloaderConfig(FsspecDownloaderConfig):
    remote_url: str = Field(description="Remote fsspec URL formatted as `protocol://dir/path`")


@dataclass
class SftpDownloader(FsspecDownloader):
    protocol: str = "sftp"
    connection_config: SftpConnectionConfig
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    download_config: Optional[SftpDownloaderConfig] = field(default_factory=SftpDownloaderConfig)

    def __post_init__(self):
        parsed_url = urlparse(self.download_config.remote_url)
        self.connection_config.host = parsed_url.hostname or self.connection_config.host
        self.connection_config.port = parsed_url.port or self.connection_config.port


class SftpUploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class SftpUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: SftpConnectionConfig
    upload_config: SftpUploaderConfig = field(default=None)


sftp_source_entry = SourceRegistryEntry(
    indexer=SftpIndexer,
    indexer_config=SftpIndexerConfig,
    downloader=SftpDownloader,
    downloader_config=SftpDownloaderConfig,
    connection_config=SftpConnectionConfig,
)

sftp_destination_entry = DestinationRegistryEntry(
    uploader=SftpUploader,
    uploader_config=SftpUploaderConfig,
    connection_config=SftpConnectionConfig,
    upload_stager_config=BlobStoreUploadStagerConfig,
    upload_stager=BlobStoreUploadStager,
)
