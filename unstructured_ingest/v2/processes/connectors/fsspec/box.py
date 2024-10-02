from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import Any, Generator, Optional

from dateutil import parser
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

CONNECTOR_TYPE = "box"


class BoxIndexerConfig(FsspecIndexerConfig):
    pass


class BoxAccessConfig(FsspecAccessConfig):
    box_app_config: Optional[str] = Field(
        default=None, description="Path to Box app credentials as json file."
    )


class BoxConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = field(default_factory=lambda: ["box"], init=False)
    access_config: Secret[BoxAccessConfig] = Field(default=BoxAccessConfig(), validate_default=True)
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    def get_access_config(self) -> dict[str, Any]:
        # Return access_kwargs with oauth. The oauth object cannot be stored directly in the config
        # because it is not serializable.
        from boxsdk import JWTAuth

        ac = self.access_config.get_secret_value()
        access_kwargs_with_oauth: dict[str, Any] = {
            "oauth": JWTAuth.from_settings_file(
                ac.box_app_config,
            ),
        }
        access_config: dict[str, Any] = ac.dict()
        access_config.pop("box_app_config", None)
        access_kwargs_with_oauth.update(access_config)

        return access_kwargs_with_oauth


@dataclass
class BoxIndexer(FsspecIndexer):
    connection_config: BoxConnectionConfig
    index_config: BoxIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["boxfs"], extras="box")
    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        return super().run(**kwargs)

    @requires_dependencies(["boxfs"], extras="box")
    def precheck(self) -> None:
        super().precheck()

    def get_metadata(self, file_data: dict) -> FileDataSourceMetadata:
        path = file_data["name"]
        date_created = None
        date_modified = None
        if modified_at_str := file_data.get("modified_at"):
            date_modified = parser.parse(modified_at_str).timestamp()
        if created_at_str := file_data.get("created_at"):
            date_created = parser.parse(created_at_str).timestamp()

        file_size = file_data.get("size") if "size" in file_data else None

        version = file_data.get("id")
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


class BoxDownloaderConfig(FsspecDownloaderConfig):
    pass


@dataclass
class BoxDownloader(FsspecDownloader):
    protocol: str = "box"
    connection_config: BoxConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[BoxDownloaderConfig] = field(default_factory=BoxDownloaderConfig)

    @requires_dependencies(["boxfs"], extras="box")
    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return super().run(file_data=file_data, **kwargs)

    @requires_dependencies(["boxfs"], extras="box")
    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return await super().run_async(file_data=file_data, **kwargs)


class BoxUploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class BoxUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: BoxConnectionConfig
    upload_config: BoxUploaderConfig = field(default=None)

    @requires_dependencies(["boxfs"], extras="box")
    def __post_init__(self):
        super().__post_init__()

    @requires_dependencies(["boxfs"], extras="box")
    def precheck(self) -> None:
        super().precheck()

    @requires_dependencies(["boxfs"], extras="box")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        return super().run(path=path, file_data=file_data, **kwargs)

    @requires_dependencies(["boxfs"], extras="box")
    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        return await super().run_async(path=path, file_data=file_data, **kwargs)


box_source_entry = SourceRegistryEntry(
    indexer=BoxIndexer,
    indexer_config=BoxIndexerConfig,
    downloader=BoxDownloader,
    downloader_config=BoxDownloaderConfig,
    connection_config=BoxConnectionConfig,
)

box_destination_entry = DestinationRegistryEntry(
    uploader=BoxUploader,
    uploader_config=BoxUploaderConfig,
    connection_config=BoxConnectionConfig,
)
