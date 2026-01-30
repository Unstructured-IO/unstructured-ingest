from __future__ import annotations

import contextlib
import hashlib
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional
from urllib.parse import urlparse

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileDataSourceMetadata
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


def _strip_leading_slash(path: str) -> str:
    """Strip one leading slash, preserving double-slash absolute path indicators.
    Example: sftp://host//home → /home (absolute), sftp://host/data → data (relative)."""
    return path[1:] if path.startswith("/") else path


# Patch hashlib.md5 for FIPS-enabled OpenSSL (common in Kubernetes).
# Paramiko uses MD5 solely for logging human-readable host key fingerprints,
# not for any cryptographic purpose (SSH security uses Ed25519/SHA-256).
# This flag tells OpenSSL the MD5 call is non-cryptographic, which is safe.
_original_md5 = hashlib.md5

def _fips_safe_md5(data=b"", **kwargs):
    kwargs.setdefault("usedforsecurity", False)
    return _original_md5(data, **kwargs)

hashlib.md5 = _fips_safe_md5


class SftpIndexerConfig(FsspecIndexerConfig):
    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        _, ext = os.path.splitext(self.remote_url)
        parsed_url = urlparse(self.remote_url)
        if ext:
            parent_path = Path(parsed_url.path).parent.as_posix()
            self.path_without_protocol = _strip_leading_slash(parent_path)
        else:
            self.path_without_protocol = _strip_leading_slash(parsed_url.path)


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
        # never gets closed so explicitly adding that as part of this context manager.
        # skip_instance_cache=True is required to prevent fsspec from returning a cached
        # instance whose SSH connection was closed by a previous context manager exit.
        from fsspec import get_filesystem_class

        client: SFTPFileSystem = get_filesystem_class(protocol)(
            skip_instance_cache=True,
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
        super().__post_init__()
        parsed_url = urlparse(self.index_config.remote_url)
        self.connection_config.host = parsed_url.hostname or self.connection_config.host
        self.connection_config.port = parsed_url.port or self.connection_config.port

    def precheck(self) -> None:
        self.log_operation_start(
            "Connection validation",
            protocol=self.index_config.protocol,
            path=self.index_config.path_without_protocol,
        )

        try:
            with self.connection_config.get_client(protocol=self.index_config.protocol) as client:
                files = client.ls(path=self.index_config.path_without_protocol, detail=True)
                valid_files = [x.get("name") for x in files if x.get("type") == "file"]
                if not valid_files:
                    self.log_operation_complete("Connection validation", count=0)
                    return
                file_to_sample = valid_files[0]
                self.log_debug(f"attempting to make HEAD request for file: {file_to_sample}")
                client.head(path=file_to_sample)

            self.log_connection_validated(
                connector_type=self.connector_type,
                endpoint=f"{self.index_config.protocol}://{self.index_config.path_without_protocol}",
            )

        except Exception as e:
            self.log_connection_failed(
                connector_type=self.connector_type,
                error=e,
                endpoint=f"{self.index_config.protocol}://{self.index_config.path_without_protocol}",
            )
            raise self.wrap_error(e=e)

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
        super().__post_init__()
        parsed_url = urlparse(self.download_config.remote_url)
        self.connection_config.host = parsed_url.hostname or self.connection_config.host
        self.connection_config.port = parsed_url.port or self.connection_config.port


class SftpUploaderConfig(FsspecUploaderConfig):
    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        parsed_url = urlparse(self.remote_url)
        self.path_without_protocol = _strip_leading_slash(parsed_url.path)


@dataclass
class SftpUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: SftpConnectionConfig
    upload_config: SftpUploaderConfig = field(default=None)

    def __post_init__(self):
        super().__post_init__()
        parsed_url = urlparse(self.upload_config.remote_url)
        self.connection_config.host = parsed_url.hostname or self.connection_config.host
        self.connection_config.port = parsed_url.port or self.connection_config.port

    def precheck(self) -> None:
        self.log_operation_start("Connection validation", protocol=self.upload_config.protocol)

        try:
            with self.connection_config.get_client(protocol=self.upload_config.protocol) as client:
                upload_path = Path(self.upload_config.path_without_protocol) / "_empty"
                client.write_bytes(path=upload_path.as_posix(), value=b"")
                # Best-effort cleanup - don't fail if user lacks delete permissions
                with contextlib.suppress(Exception):
                    client.rm(path=upload_path.as_posix())

            self.log_connection_validated(
                connector_type=self.connector_type,
                endpoint=f"{self.upload_config.protocol}://{self.upload_config.path_without_protocol}",
            )

        except Exception as e:
            self.log_connection_failed(
                connector_type=self.connector_type,
                error=e,
                endpoint=f"{self.upload_config.protocol}://{self.upload_config.path_without_protocol}",
            )
            raise self.wrap_error(e=e)


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
