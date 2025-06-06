import glob
import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import Any, Generator

from pydantic import Field, Secret

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
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.processes.utils.blob_storage import (
    BlobStoreUploadStager,
    BlobStoreUploadStagerConfig,
)

CONNECTOR_TYPE = "local"


class LocalAccessConfig(AccessConfig):
    pass


class LocalConnectionConfig(ConnectionConfig):
    access_config: Secret[LocalAccessConfig] = Field(
        default=LocalAccessConfig(), validate_default=True
    )


class LocalIndexerConfig(IndexerConfig):
    input_path: Path = Field(
        description="Path to the location in the local file system that will be processed."
    )
    recursive: bool = Field(
        default=False,
        description="Recursively download files in their respective folders "
        "otherwise stop at the files in provided folder level.",
    )

    @property
    def path(self) -> Path:
        return Path(self.input_path).resolve()


@dataclass
class LocalIndexer(Indexer):
    index_config: LocalIndexerConfig
    connection_config: LocalConnectionConfig = field(
        default_factory=lambda: LocalConnectionConfig()
    )
    connector_type: str = CONNECTOR_TYPE

    def list_files(self) -> list[Path]:
        input_path = self.index_config.path
        if input_path.is_file():
            return [Path(s) for s in glob.glob(f"{self.index_config.path}")]
        files = []
        if self.index_config.recursive:
            files.extend(list(input_path.rglob("*")))
        else:
            files.extend(list(input_path.glob("*")))
        return [f for f in files if f.is_file()]

    def get_file_metadata(self, path: Path) -> FileDataSourceMetadata:
        stats = path.stat()
        try:
            date_modified = str(stats.st_mtime)
        except Exception as e:
            logger.warning(f"Couldn't detect date modified: {e}")
            date_modified = None

        try:
            date_created = str(stats.st_birthtime)
        except Exception as e:
            logger.warning(f"Couldn't detect date created: {e}")
            date_created = None

        try:
            mode = stats.st_mode
            permissions_data = [{"mode": mode}]
        except Exception as e:
            logger.warning(f"Couldn't detect file mode: {e}")
            permissions_data = None

        try:
            filesize_bytes = stats.st_size
        except Exception as e:
            logger.warning(f"Couldn't detect file size: {e}")
            filesize_bytes = None

        return FileDataSourceMetadata(
            date_modified=date_modified,
            date_created=date_created,
            date_processed=str(time()),
            permissions_data=permissions_data,
            record_locator={"path": str(path.resolve())},
            filesize_bytes=filesize_bytes,
        )

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        for file_path in self.list_files():
            source_identifiers = SourceIdentifiers(
                fullpath=str(file_path.resolve()),
                filename=file_path.name,
                rel_path=(
                    str(file_path.resolve()).replace(str(self.index_config.path.resolve()), "")[1:]
                    if not self.index_config.path.is_file()
                    else self.index_config.path.name
                ),
            )
            file_data = FileData(
                identifier=str(file_path.resolve()),
                connector_type=CONNECTOR_TYPE,
                source_identifiers=source_identifiers,
                metadata=self.get_file_metadata(path=file_path),
                display_name=source_identifiers.fullpath,
            )
            yield file_data


class LocalDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class LocalDownloader(Downloader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: LocalConnectionConfig = field(default_factory=LocalConnectionConfig)
    download_config: LocalDownloaderConfig = field(default_factory=LocalDownloaderConfig)

    def get_download_path(self, file_data: FileData) -> Path:
        return Path(file_data.source_identifiers.fullpath)

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return DownloadResponse(
            file_data=file_data, path=Path(file_data.source_identifiers.fullpath)
        )


class LocalUploaderConfig(UploaderConfig):
    output_dir: str = Field(
        default="structured-output", description="Local path to write partitioned output to"
    )

    @property
    def output_path(self) -> Path:
        return Path(self.output_dir).resolve()

    def __post_init__(self):
        if self.output_path.exists() and self.output_path.is_file():
            raise ValueError("output path already exists as a file")


@dataclass
class LocalUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: LocalUploaderConfig = field(default_factory=LocalUploaderConfig)
    connection_config: LocalConnectionConfig = field(
        default_factory=lambda: LocalConnectionConfig()
    )

    def is_async(self) -> bool:
        return False

    def get_destination_path(self, file_data: FileData) -> Path:
        if source_identifiers := file_data.source_identifiers:
            rel_path = (
                source_identifiers.relative_path[1:]
                if source_identifiers.relative_path.startswith("/")
                else source_identifiers.relative_path
            )
            new_path = self.upload_config.output_path / Path(rel_path)
            final_path = str(new_path).replace(
                source_identifiers.filename, f"{source_identifiers.filename}.json"
            )
        else:
            final_path = self.upload_config.output_path / Path(f"{file_data.identifier}.json")
        final_path = Path(final_path)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        return final_path

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        final_path = self.get_destination_path(file_data=file_data)
        with final_path.open("w") as f:
            json.dump(data, f)

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        final_path = self.get_destination_path(file_data=file_data)
        logger.debug(f"copying file from {path} to {final_path}")
        shutil.copy(src=str(path), dst=str(final_path))


local_source_entry = SourceRegistryEntry(
    indexer=LocalIndexer,
    indexer_config=LocalIndexerConfig,
    downloader=LocalDownloader,
    downloader_config=LocalDownloaderConfig,
    connection_config=LocalConnectionConfig,
)

local_destination_entry = DestinationRegistryEntry(
    uploader=LocalUploader,
    uploader_config=LocalUploaderConfig,
    upload_stager_config=BlobStoreUploadStagerConfig,
    upload_stager=BlobStoreUploadStager,
)
