from __future__ import annotations

import os
import random
import shutil
import tempfile
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
    sample_n_files: Optional[int] = None


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

        if self.index_config.sample_n_files:
            filtered_files = self.sample_n_files(filtered_files, self.index_config.sample_n_files)

        return filtered_files

    def sample_n_files(self, files: list[dict[str, Any]], n) -> list[dict[str, Any]]:
        if len(files) <= n:
            logger.warning(
                f"number of files to be sampled={n} is not smaller than the number"
                f" of files found ({len(files)}). Returning all of the files as the"
                " sample."
            )
            return files

        return random.sample(files, n)

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
                display_name=file_path,
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

    def handle_directory_download(self, lpath: Path) -> None:
        # If the object's name contains certain characters (i.e. '?'), it
        # gets downloaded into a new directory of the same name. This
        # reconciles that with what is expected, which is to download it
        # as a file that is not within a directory.
        if not lpath.is_dir():
            return
        desired_name = lpath.name
        files_in_dir = [file for file in lpath.iterdir() if file.is_file()]
        if not files_in_dir:
            raise ValueError(f"no files in {lpath}")
        if len(files_in_dir) > 1:
            raise ValueError(
                "Multiple files in {}: {}".format(lpath, ", ".join([str(f) for f in files_in_dir]))
            )
        file = files_in_dir[0]
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_location = os.path.join(temp_dir, desired_name)
            shutil.copyfile(src=file, dst=temp_location)
            shutil.rmtree(lpath)
            shutil.move(src=temp_location, dst=lpath)

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            rpath = file_data.additional_metadata["original_file_path"]
            self.fs.get(rpath=rpath, lpath=download_path.as_posix())
            self.handle_directory_download(lpath=download_path)
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
            self.handle_directory_download(lpath=download_path)
        except Exception as e:
            logger.error(f"failed to download file {file_data.identifier}: {e}", exc_info=True)
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")
        return self.generate_download_response(file_data=file_data, download_path=download_path)


class FsspecUploaderConfig(FileConfig, UploaderConfig):
    pass


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
        logger.debug(f"writing local file {path_str} to {upload_path}")
        self.fs.upload(lpath=path_str, rpath=str(upload_path))

    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        upload_path = self.get_upload_path(file_data=file_data)
        path_str = str(path.resolve())
        # Odd that fsspec doesn't run exists() as async even when client support async
        logger.debug(f"writing local file {path_str} to {upload_path}")
        self.fs.upload(lpath=path_str, rpath=str(upload_path))
