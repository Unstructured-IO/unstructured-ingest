from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
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
            files = fs.ls(path=self.index_config.path_without_protocol, detail=True)
            valid_files = [x.get("name") for x in files if x.get("type") == "file"]
            if not valid_files:
                return
            file_to_sample = valid_files[0]
            logger.debug(f"attempting to make HEAD request for file: {file_to_sample}")
            self.fs.head(path=file_to_sample)
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def get_file_data(self) -> list[dict[str, Any]]:
        if not self.index_config.recursive:
            # fs.ls does not walk directories
            # directories that are listed in cloud storage can cause problems
            # because they are seen as 0 byte files
            files = self.fs.ls(self.index_config.path_without_protocol, detail=True)

        else:
            # fs.find will recursively walk directories
            # "size" is a common key for all the cloud protocols with fs
            found = self.fs.find(
                self.index_config.path_without_protocol,
                detail=True,
            )
            files = found.values()
        filtered_files = [
            file for file in files if file.get("size") > 0 and file.get("type") == "file"
        ]
        return filtered_files

    def get_metadata(self, file_data: dict) -> FileDataSourceMetadata:
        raise NotImplementedError()

    def get_path(self, file_data: dict) -> str:
        return file_data["name"]

    def sterilize_info(self, file_data: dict) -> dict:
        return sterilize_dict(data=file_data)

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        files = self.get_file_data()
        for file_data in files:
            file_path = self.get_path(file_data=file_data)
            # Note: we remove any remaining leading slashes (Box introduces these)
            # to get a valid relative path
            rel_path = file_path.replace(self.index_config.path_without_protocol, "").lstrip("/")

            additional_metadata = self.sterilize_info(file_data=file_data)
            additional_metadata["original_file_path"] = file_path
            yield FileData(
                identifier=str(uuid5(NAMESPACE_DNS, file_path)),
                connector_type=self.connector_type,
                source_identifiers=SourceIdentifiers(
                    filename=Path(file_path).name,
                    rel_path=rel_path or None,
                    fullpath=file_path,
                ),
                metadata=self.get_metadata(file_data=file_data),
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
            upload_path = Path(self.upload_config.path_without_protocol) / "_empty"
            fs.write_bytes(path=str(upload_path), value=b"")
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
            logger.debug(f"skipping upload of {path} to {upload_path}, file already exists")
            return
        logger.debug(f"writing local file {path_str} to {upload_path}")
        self.fs.upload(lpath=path_str, rpath=str(upload_path))

    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        upload_path = self.get_upload_path(file_data=file_data)
        path_str = str(path.resolve())
        # Odd that fsspec doesn't run exists() as async even when client support async
        already_exists = self.fs.exists(path=str(upload_path))
        if already_exists and not self.upload_config.overwrite:
            logger.debug(f"skipping upload of {path} to {upload_path}, file already exists")
            return
        logger.debug(f"writing local file {path_str} to {upload_path}")
        self.fs.upload(lpath=path_str, rpath=str(upload_path))
