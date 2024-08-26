from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generator, Optional
from urllib.parse import urlparse

from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import DownloadResponse, FileData
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


@dataclass
class SftpIndexer(FsspecIndexer):
    connection_config: SftpConnectionConfig
    index_config: SftpIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    def __post_init__(self):
        parsed_url = urlparse(self.index_config.remote_url)
        self.connection_config.host = parsed_url.hostname or self.connection_config.host
        self.connection_config.port = parsed_url.port or self.connection_config.port

    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
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

    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    def precheck(self) -> None:
        super().precheck()


class SftpDownloaderConfig(FsspecDownloaderConfig):
    remote_url: str = Field(description="Remote fsspec URL formatted as `protocol://dir/path`")


@dataclass
class SftpDownloader(FsspecDownloader):
    protocol: str = "sftp"
    connection_config: SftpConnectionConfig
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    download_config: Optional[SftpDownloaderConfig] = field(default_factory=SftpDownloaderConfig)

    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    def __post_init__(self):
        parsed_url = urlparse(self.download_config.remote_url)
        self.connection_config.host = parsed_url.hostname or self.connection_config.host
        self.connection_config.port = parsed_url.port or self.connection_config.port

    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return super().run(file_data=file_data, **kwargs)

    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return await super().run_async(file_data=file_data, **kwargs)


class SftpUploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class SftpUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: SftpConnectionConfig
    upload_config: SftpUploaderConfig = field(default=None)

    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    def __post_init__(self):
        super().__post_init__()

    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    def precheck(self) -> None:
        super().precheck()

    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        return super().run(path=path, file_data=file_data, **kwargs)

    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        return await super().run_async(path=path, file_data=file_data, **kwargs)


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
)
