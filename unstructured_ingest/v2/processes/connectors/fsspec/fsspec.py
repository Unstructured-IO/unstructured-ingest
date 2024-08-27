from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional, TypeVar
from uuid import NAMESPACE_DNS, uuid5

from pydantic import BaseModel, Field, Secret

from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
)
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
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connectors.fsspec.utils import sterilize_dict

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

CONNECTOR_TYPE = "fsspec"


class FileConfig(BaseModel):
    remote_url: str = Field(description="Remote fsspec URL formatted as `protocol://dir/path`")
    protocol: str = Field(init=False)
    path_without_protocol: str = Field(init=False)
    supported_protocols: list[str] = Field(
        init=False,
        default_factory=lambda: [
            "s3",
            "s3a",
            "abfs",
            "az",
            "gs",
            "gcs",
            "box",
            "dropbox",
            "sftp",
        ],
    )

    def __init__(self, **data):
        protocol, path_without_protocol = data["remote_url"].split("://")
        data["protocol"] = protocol
        data["path_without_protocol"] = path_without_protocol
        super().__init__(**data)


class FsspecIndexerConfig(FileConfig, IndexerConfig):
    recursive: bool = False


class FsspecAccessConfig(AccessConfig):
    pass


class FsspecConnectionConfig(ConnectionConfig):
    access_config: Secret[FsspecAccessConfig]
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)


FsspecIndexerConfigT = TypeVar("FsspecIndexerConfigT", bound=FsspecIndexerConfig)
FsspecConnectionConfigT = TypeVar("FsspecConnectionConfigT", bound=FsspecConnectionConfig)


@dataclass
class FsspecIndexer(Indexer):
    connection_config: FsspecConnectionConfigT
    index_config: FsspecIndexerConfigT
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @property
    def fs(self) -> "AbstractFileSystem":
        from fsspec import get_filesystem_class

        return get_filesystem_class(self.index_config.protocol)(
            **self.connection_config.get_access_config(),
        )

    def precheck(self) -> None:
        from fsspec import get_filesystem_class

        try:
            fs = get_filesystem_class(self.index_config.protocol)(
                **self.connection_config.get_access_config(),
            )
            fs.ls(path=self.index_config.path_without_protocol, detail=False)
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def list_files(self) -> list[str]:
        if not self.index_config.recursive:
            # fs.ls does not walk directories
            # directories that are listed in cloud storage can cause problems
            # because they are seen as 0 byte files
            found = self.fs.ls(self.index_config.path_without_protocol, detail=True)
            if isinstance(found, list):
                return [
                    x.get("name") for x in found if x.get("size") > 0 and x.get("type") == "file"
                ]
            else:
                raise TypeError(f"unhandled response type from ls: {type(found)}")
        else:
            # fs.find will recursively walk directories
            # "size" is a common key for all the cloud protocols with fs
            found = self.fs.find(
                self.index_config.path_without_protocol,
                detail=True,
            )
            if isinstance(found, dict):
                return [
                    k for k, v in found.items() if v.get("size") > 0 and v.get("type") == "file"
                ]
            else:
                raise TypeError(f"unhandled response type from find: {type(found)}")

    def get_metadata(self, path: str) -> FileDataSourceMetadata:
        date_created = None
        date_modified = None
        file_size = None
        try:
            created: Optional[Any] = self.fs.created(path)
            if created:
                if isinstance(created, datetime):
                    date_created = str(created.timestamp())
                else:
                    date_created = str(created)
        except NotImplementedError:
            pass

        try:
            modified: Optional[Any] = self.fs.modified(path)
            if modified:
                if isinstance(modified, datetime):
                    date_modified = str(modified.timestamp())
                else:
                    date_modified = str(modified)
        except NotImplementedError:
            pass
        with contextlib.suppress(AttributeError):
            file_size = self.fs.size(path)

        version = self.fs.checksum(path)
        metadata: dict[str, str] = {}
        with contextlib.suppress(AttributeError):
            metadata = self.fs.metadata(path)
        record_locator = {
            "protocol": self.index_config.protocol,
            "remote_file_path": self.index_config.remote_url,
        }
        file_stat = self.fs.stat(path=path)
        if file_id := file_stat.get("id"):
            record_locator["file_id"] = file_id
        if metadata:
            record_locator["metadata"] = metadata
        return FileDataSourceMetadata(
            date_created=date_created,
            date_modified=date_modified,
            date_processed=str(time()),
            version=str(version),
            url=f"{self.index_config.protocol}://{path}",
            record_locator=record_locator,
            filesize_bytes=file_size,
        )

    def sterilize_info(self, path) -> dict:
        info = self.fs.info(path=path)
        return sterilize_dict(data=info)

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        files = self.list_files()
        for file in files:
            # Note: we remove any remaining leading slashes (Box introduces these)
            # to get a valid relative path
            rel_path = file.replace(self.index_config.path_without_protocol, "").lstrip("/")

            additional_metadata = self.sterilize_info(path=file)
            additional_metadata["original_file_path"] = file
            yield FileData(
                identifier=str(uuid5(NAMESPACE_DNS, file)),
                connector_type=self.connector_type,
                source_identifiers=SourceIdentifiers(
                    filename=Path(file).name,
                    rel_path=rel_path or None,
                    fullpath=file,
                ),
                metadata=self.get_metadata(path=file),
                additional_metadata=additional_metadata,
            )


class FsspecDownloaderConfig(DownloaderConfig):
    pass


FsspecDownloaderConfigT = TypeVar("FsspecDownloaderConfigT", bound=FsspecDownloaderConfig)


@dataclass
class FsspecDownloader(Downloader):
    protocol: str
    connection_config: FsspecConnectionConfigT
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[FsspecDownloaderConfigT] = field(
        default_factory=lambda: FsspecDownloaderConfig()
    )

    def is_async(self) -> bool:
        return self.fs.async_impl

    @property
    def fs(self) -> "AbstractFileSystem":
        from fsspec import get_filesystem_class

        return get_filesystem_class(self.protocol)(
            **self.connection_config.get_access_config(),
        )

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            rpath = file_data.additional_metadata["original_file_path"]
            self.fs.get(rpath=rpath, lpath=download_path.as_posix())
        except Exception as e:
            logger.error(f"failed to download file {file_data.identifier}: {e}", exc_info=True)
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")
        return self.generate_download_response(file_data=file_data, download_path=download_path)

    async def async_run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            rpath = file_data.additional_metadata["original_file_path"]
            await self.fs.get(rpath=rpath, lpath=download_path.as_posix())
        except Exception as e:
            logger.error(f"failed to download file {file_data.identifier}: {e}", exc_info=True)
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")
        return self.generate_download_response(file_data=file_data, download_path=download_path)


class FsspecUploaderConfig(FileConfig, UploaderConfig):
    overwrite: bool = Field(
        default=False, description="If true, an existing file will be overwritten."
    )


FsspecUploaderConfigT = TypeVar("FsspecUploaderConfigT", bound=FsspecUploaderConfig)


@dataclass
class FsspecUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: FsspecUploaderConfigT = field(default=None)

    def is_async(self) -> bool:
        return self.fs.async_impl

    @property
    def fs(self) -> "AbstractFileSystem":
        from fsspec import get_filesystem_class

        fs_kwargs = self.connection_config.get_access_config() if self.connection_config else {}
        return get_filesystem_class(self.upload_config.protocol)(
            **fs_kwargs,
        )

    def __post_init__(self):
        # TODO once python3.9 no longer supported and kw_only is allowed in dataclasses, remove:
        if not self.upload_config:
            raise TypeError(
                f"{self.__class__.__name__}.__init__() "
                f"missing 1 required positional argument: 'upload_config'"
            )

    def precheck(self) -> None:
        from fsspec import get_filesystem_class

        try:
            fs = get_filesystem_class(self.upload_config.protocol)(
                **self.connection_config.get_access_config(),
            )
            root_dir = self.upload_config.path_without_protocol.split("/")[0]
            fs.ls(path=root_dir, detail=False)
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def get_upload_path(self, file_data: FileData) -> Path:
        upload_path = (
            Path(self.upload_config.path_without_protocol)
            / file_data.source_identifiers.relative_path
        )
        updated_upload_path = upload_path.parent / f"{upload_path.name}.json"
        return updated_upload_path

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        path_str = str(path.resolve())
        upload_path = self.get_upload_path(file_data=file_data)
        if self.fs.exists(path=str(upload_path)) and not self.upload_config.overwrite:
            logger.debug(f"Skipping upload of {path} to {upload_path}, file already exists")
            return
        logger.debug(f"Writing local file {path_str} to {upload_path}")
        self.fs.upload(lpath=path_str, rpath=str(upload_path))

    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        upload_path = self.get_upload_path(file_data=file_data)
        path_str = str(path.resolve())
        # Odd that fsspec doesn't run exists() as async even when client support async
        already_exists = self.fs.exists(path=str(upload_path))
        if already_exists and not self.upload_config.overwrite:
            logger.debug(f"Skipping upload of {path} to {upload_path}, file already exists")
            return
        logger.debug(f"Writing local file {path_str} to {upload_path}")
        self.fs.upload(lpath=path_str, rpath=str(upload_path))
