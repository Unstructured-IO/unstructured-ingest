import contextlib
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
)
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

CONNECTOR_TYPE = "s3"


class S3IndexerConfig(FsspecIndexerConfig):
    pass


class S3AccessConfig(FsspecAccessConfig):
    key: Optional[str] = Field(
        default=None,
        description="If not anonymous, use this access key ID, if specified. Takes precedence "
        "over `aws_access_key_id` in client_kwargs.",
    )
    secret: Optional[str] = Field(
        default=None, description="If not anonymous, use this secret access key, if specified."
    )
    token: Optional[str] = Field(
        default=None, description="If not anonymous, use this security token, if specified."
    )


class S3ConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = field(default_factory=lambda: ["s3", "s3a"], init=False)
    access_config: Secret[S3AccessConfig] = Field(default=S3AccessConfig(), validate_default=True)
    endpoint_url: Optional[str] = Field(
        default=None,
        description="Use this endpoint_url, if specified. Needed for "
        "connecting to non-AWS S3 buckets.",
    )
    anonymous: bool = Field(
        default=False, description="Connect to s3 without local AWS credentials."
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    def get_access_config(self) -> dict[str, Any]:
        access_configs: dict[str, Any] = {"anon": self.anonymous}
        if self.endpoint_url:
            access_configs["endpoint_url"] = self.endpoint_url

        # Avoid injecting None by filtering out k,v pairs where the value is None
        access_configs.update(
            {k: v for k, v in self.access_config.get_secret_value().dict().items() if v}
        )
        return access_configs


@dataclass
class S3Indexer(FsspecIndexer):
    connection_config: S3ConnectionConfig
    index_config: S3IndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def get_path(self, file_data: dict) -> str:
        return file_data["Key"]

    def get_metadata(self, file_data: dict) -> FileDataSourceMetadata:
        path = file_data["Key"]
        date_created = None
        date_modified = None
        modified = file_data.get("LastModified")
        if modified:
            date_created = str(modified.timestamp())
            date_modified = str(modified.timestamp())

        file_size = file_data.get("size") if "size" in file_data else None
        file_size = file_size or file_data.get("Size")

        version = file_data.get("ETag").rstrip('"').lstrip('"') if "ETag" in file_data else None
        metadata: dict[str, str] = {}
        with contextlib.suppress(AttributeError):
            metadata = self.fs.metadata(path=path)
        record_locator = {
            "protocol": self.index_config.protocol,
            "remote_file_path": self.index_config.remote_url,
        }
        if metadata:
            record_locator["metadata"] = metadata
        return FileDataSourceMetadata(
            date_created=date_created,
            date_modified=date_modified,
            date_processed=str(time()),
            version=version,
            url=f"{self.index_config.protocol}://{path}",
            record_locator=record_locator,
            filesize_bytes=file_size,
        )

    @requires_dependencies(["s3fs", "fsspec"], extras="s3")
    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        return super().run(**kwargs)

    @requires_dependencies(["s3fs", "fsspec"], extras="s3")
    def precheck(self) -> None:
        super().precheck()


class S3DownloaderConfig(FsspecDownloaderConfig):
    pass


@dataclass
class S3Downloader(FsspecDownloader):
    protocol: str = "s3"
    connection_config: S3ConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[S3DownloaderConfig] = field(default_factory=S3DownloaderConfig)

    @requires_dependencies(["s3fs", "fsspec"], extras="s3")
    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return super().run(file_data=file_data, **kwargs)

    @requires_dependencies(["s3fs", "fsspec"], extras="s3")
    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return await super().run_async(file_data=file_data, **kwargs)


class S3UploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class S3Uploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: S3ConnectionConfig
    upload_config: S3UploaderConfig = field(default=None)

    @requires_dependencies(["s3fs", "fsspec"], extras="s3")
    def precheck(self) -> None:
        super().precheck()

    @requires_dependencies(["s3fs", "fsspec"], extras="s3")
    def __post_init__(self):
        super().__post_init__()

    @requires_dependencies(["s3fs", "fsspec"], extras="s3")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        return super().run(path=path, file_data=file_data, **kwargs)

    @requires_dependencies(["s3fs", "fsspec"], extras="s3")
    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        return await super().run_async(path=path, file_data=file_data, **kwargs)


s3_source_entry = SourceRegistryEntry(
    indexer=S3Indexer,
    indexer_config=S3IndexerConfig,
    downloader=S3Downloader,
    downloader_config=S3DownloaderConfig,
    connection_config=S3ConnectionConfig,
)

s3_destination_entry = DestinationRegistryEntry(
    uploader=S3Uploader,
    uploader_config=S3UploaderConfig,
    connection_config=S3ConnectionConfig,
)
