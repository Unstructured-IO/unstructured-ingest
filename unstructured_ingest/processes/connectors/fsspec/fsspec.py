from __future__ import annotations

import os
import random
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional, TypeVar
from uuid import NAMESPACE_DNS, uuid5

from pydantic import BaseModel, Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.processes.connectors.fsspec.utils import sterilize_dict
from unstructured_ingest.utils.filesystem import mkdir_concurrent_safe

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

    @contextmanager
    def get_client(self, protocol: str) -> Generator["AbstractFileSystem", None, None]:
        from fsspec import get_filesystem_class

        client = get_filesystem_class(protocol)(
            **self.get_access_config(),
        )
        yield client

    def wrap_error(self, e: Exception) -> Exception:
        return e


FsspecIndexerConfigT = TypeVar("FsspecIndexerConfigT", bound=FsspecIndexerConfig)
FsspecConnectionConfigT = TypeVar("FsspecConnectionConfigT", bound=FsspecConnectionConfig)


@dataclass
class FsspecIndexer(Indexer):
    connection_config: FsspecConnectionConfigT
    index_config: FsspecIndexerConfigT
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    def wrap_error(self, e: Exception) -> Exception:
        return self.connection_config.wrap_error(e=e)

    def precheck(self) -> None:
        from fsspec import get_filesystem_class

        self.log_operation_start(
            "Connection validation",
            protocol=self.index_config.protocol,
            path=self.index_config.path_without_protocol,
        )

        try:
            fs = get_filesystem_class(self.index_config.protocol)(
                **self.connection_config.get_access_config(),
            )
            files = fs.ls(path=self.index_config.path_without_protocol, detail=True)
            valid_files = [x.get("name") for x in files if x.get("type") == "file"]
            if not valid_files:
                self.log_operation_complete("Connection validation", count=0)
                return
            file_to_sample = valid_files[0]
            self.log_debug(f"attempting to make HEAD request for file: {file_to_sample}")
            with self.connection_config.get_client(protocol=self.index_config.protocol) as client:
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

    def get_file_info(self) -> list[dict[str, Any]]:
        if not self.index_config.recursive:
            # fs.ls does not walk directories
            # directories that are listed in cloud storage can cause problems
            # because they are seen as 0 byte files
            with self.connection_config.get_client(protocol=self.index_config.protocol) as client:
                files = client.ls(self.index_config.path_without_protocol, detail=True)

        else:
            # fs.find will recursively walk directories
            # "size" is a common key for all the cloud protocols with fs
            with self.connection_config.get_client(protocol=self.index_config.protocol) as client:
                found = client.find(
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
            self.log_warning(
                f"number of files to be sampled={n} is not smaller than the number"
                f" of files found ({len(files)}). Returning all of the files as the"
                " sample."
            )
            return files

        return random.sample(files, n)

    def get_metadata(self, file_info: dict) -> FileDataSourceMetadata:
        raise NotImplementedError()

    def get_path(self, file_info: dict) -> str:
        return file_info["name"]

    def sterilize_info(self, file_data: dict) -> dict:
        return sterilize_dict(data=file_data)

    def create_init_file_data(self, remote_filepath: Optional[str] = None) -> FileData:
        # Create initial file data that requires no network calls and is constructed purely
        # with information that exists in the config
        remote_filepath = remote_filepath or self.index_config.remote_url
        path_without_protocol = remote_filepath.split("://")[1]
        rel_path = remote_filepath.replace(path_without_protocol, "").lstrip("/")
        return FileData(
            identifier=str(uuid5(NAMESPACE_DNS, remote_filepath)),
            connector_type=self.connector_type,
            display_name=remote_filepath,
            source_identifiers=SourceIdentifiers(
                filename=Path(remote_filepath).name,
                rel_path=rel_path or None,
                fullpath=remote_filepath,
            ),
            metadata=FileDataSourceMetadata(url=remote_filepath),
        )

    def hydrate_file_data(self, init_file_data: FileData):
        # Get file info
        with self.connection_config.get_client(protocol=self.index_config.protocol) as client:
            files = client.ls(self.index_config.path_without_protocol, detail=True)
        filtered_files = [
            file for file in files if file.get("size") > 0 and file.get("type") == "file"
        ]
        if not filtered_files:
            raise ValueError(f"{init_file_data} did not reference any valid file")
        if len(filtered_files) > 1:
            raise ValueError(f"{init_file_data} referenced more than one file")
        file_info = filtered_files[0]
        init_file_data.additional_metadata = self.get_metadata(file_info=file_info)

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        self.log_indexing_start(f"{self.connector_type} files")

        files = self.get_file_info()
        total_files = len(files)

        self.log_operation_start("File indexing", total_files=total_files)

        for i, file_info in enumerate(files):
            file_path = self.get_path(file_info=file_info)

            # Only log progress for larger operations
            if total_files > 5:
                self.log_progress(
                    current=i + 1, total=total_files, item_type="files", operation="Indexing"
                )

            # Note: we remove any remaining leading slashes (Box introduces these)
            # to get a valid relative path
            rel_path = file_path.replace(self.index_config.path_without_protocol, "").lstrip("/")

            additional_metadata = self.sterilize_info(file_data=file_info)
            additional_metadata["original_file_path"] = file_path
            yield FileData(
                identifier=str(uuid5(NAMESPACE_DNS, file_path)),
                connector_type=self.connector_type,
                source_identifiers=SourceIdentifiers(
                    filename=Path(file_path).name,
                    rel_path=rel_path or None,
                    fullpath=file_path,
                ),
                metadata=self.get_metadata(file_info=file_info),
                additional_metadata=additional_metadata,
                display_name=file_path,
            )

        self.log_indexing_complete(f"{self.connector_type} files", total_files)


class FsspecDownloaderConfig(DownloaderConfig):
    pass


FsspecDownloaderConfigT = TypeVar("FsspecDownloaderConfigT", bound=FsspecDownloaderConfig)


@dataclass
class FsspecDownloader(Downloader):
    TEMP_DIR_PREFIX = "unstructured_"
    
    protocol: str
    connection_config: FsspecConnectionConfigT
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[FsspecDownloaderConfigT] = field(
        default_factory=lambda: FsspecDownloaderConfig()
    )
    
    def get_download_path(self, file_data: FileData) -> Optional[Path]:
        has_source_identifiers = file_data.source_identifiers is not None
        has_filename = has_source_identifiers and file_data.source_identifiers.filename
        
        if not (has_source_identifiers and has_filename):
            return None
        
        filename = file_data.source_identifiers.filename
        
        mkdir_concurrent_safe(self.download_dir)
        
        temp_dir = tempfile.mkdtemp(
            prefix=self.TEMP_DIR_PREFIX, 
            dir=self.download_dir
        )
        return Path(temp_dir) / filename

    def is_async(self) -> bool:
        with self.connection_config.get_client(protocol=self.protocol) as client:
            return client.async_impl

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

    def wrap_error(self, e: Exception) -> Exception:
        return self.connection_config.wrap_error(e=e)

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        download_path = self.get_download_path(file_data=file_data)
        mkdir_concurrent_safe(download_path.parent)

        rpath = file_data.additional_metadata["original_file_path"]
        file_size = file_data.metadata.filesize_bytes
        self.log_download_start(file_path=rpath, file_id=file_data.identifier, file_size=file_size)

        try:
            with self.connection_config.get_client(protocol=self.protocol) as client:
                client.get_file(rpath=rpath, lpath=download_path.as_posix())
            self.handle_directory_download(lpath=download_path)

        except Exception as e:
            self.log_error(
                "File download failed",
                error=e,
                context={"file_path": rpath, "file_id": file_data.identifier},
            )
            raise self.wrap_error(e=e)

        self.log_download_complete(
            file_path=rpath,
            file_id=file_data.identifier,
            download_path=str(download_path),
        )

        return self.generate_download_response(file_data=file_data, download_path=download_path)

    async def async_run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        download_path = self.get_download_path(file_data=file_data)
        mkdir_concurrent_safe(download_path.parent)
        rpath = file_data.additional_metadata["original_file_path"]
        file_size = file_data.metadata.filesize_bytes
        self.log_download_start(file_path=rpath, file_id=file_data.identifier, file_size=file_size)

        try:
            with self.connection_config.get_client(protocol=self.protocol) as client:
                await client.get_file(rpath=rpath, lpath=download_path.as_posix())
            self.handle_directory_download(lpath=download_path)
        except Exception as e:
            self.log_error(
                "File download failed",
                error=e,
                context={"file_path": rpath, "file_id": file_data.identifier},
            )
            raise self.wrap_error(e=e)

        self.log_download_complete(
            file_path=rpath,
            file_id=file_data.identifier,
            download_path=str(download_path),
        )

        return self.generate_download_response(file_data=file_data, download_path=download_path)


class FsspecUploaderConfig(FileConfig, UploaderConfig):
    pass


FsspecUploaderConfigT = TypeVar("FsspecUploaderConfigT", bound=FsspecUploaderConfig)


@dataclass
class FsspecUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: FsspecUploaderConfigT = field(default=None)
    connection_config: FsspecConnectionConfigT

    def is_async(self) -> bool:
        with self.connection_config.get_client(protocol=self.upload_config.protocol) as client:
            return client.async_impl

    @property
    def fs(self) -> "AbstractFileSystem":
        from fsspec import get_filesystem_class

        fs_kwargs = self.connection_config.get_access_config() if self.connection_config else {}
        return get_filesystem_class(self.upload_config.protocol)(
            **fs_kwargs,
        )

    def __post_init__(self):
        super().__post_init__()
        # TODO: Consider using `kw_only` instead
        if not self.upload_config:
            raise TypeError(
                f"{self.__class__.__name__}.__init__() "
                f"missing 1 required positional argument: 'upload_config'"
            )

    def wrap_error(self, e: Exception) -> Exception:
        return self.connection_config.wrap_error(e=e)

    def precheck(self) -> None:
        from fsspec import get_filesystem_class

        self.log_operation_start("Connection validation", protocol=self.upload_config.protocol)

        try:
            fs = get_filesystem_class(self.upload_config.protocol)(
                **self.connection_config.get_access_config(),
            )
            upload_path = Path(self.upload_config.path_without_protocol) / "_empty"
            fs.write_bytes(path=upload_path.as_posix(), value=b"")
        except Exception as e:
            self.log_connection_failed(
                connector_type=self.connector_type,
                error=e,
                endpoint=f"{self.upload_config.protocol}://{self.upload_config.path_without_protocol}",
            )
            raise self.wrap_error(e=e)
        self.log_connection_validated(
            connector_type=self.connector_type,
            endpoint=f"{self.upload_config.protocol}://{self.upload_config.path_without_protocol}",
        )

    def get_upload_path(self, file_data: FileData) -> Path:
        upload_path = Path(
            self.upload_config.path_without_protocol
        ) / file_data.source_identifiers.relative_path.lstrip("/")
        updated_upload_path = upload_path.parent / f"{upload_path.name}.json"
        return updated_upload_path

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        path_str = str(path.resolve())
        upload_path = self.get_upload_path(file_data=file_data)
        self.log_upload_start(file_path=path_str, destination=upload_path.as_posix())
        try:
            with self.connection_config.get_client(protocol=self.upload_config.protocol) as client:
                client.upload(lpath=path_str, rpath=upload_path.as_posix())
        except Exception as e:
            self.log_error(
                "File upload failed",
                error=e,
                context={"file_path": path_str, "destination": upload_path.as_posix()},
            )
            raise self.wrap_error(e=e)
        self.log_upload_complete(file_path=path_str, destination=upload_path.as_posix())

    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        path_str = str(path.resolve())
        upload_path = self.get_upload_path(file_data=file_data)
        self.log_upload_start(file_path=path_str, destination=upload_path.as_posix())
        try:
            with self.connection_config.get_client(protocol=self.upload_config.protocol) as client:
                client.upload(lpath=path_str, rpath=upload_path.as_posix())
        except Exception as e:
            self.log_error(
                "File upload failed",
                error=e,
                context={"file_path": path_str, "destination": upload_path.as_posix()},
            )
            raise self.wrap_error(e=e)
        self.log_upload_complete(file_path=path_str, destination=upload_path.as_posix())
