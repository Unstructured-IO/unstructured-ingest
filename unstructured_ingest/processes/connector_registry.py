from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Type, TypeVar

from unstructured_ingest.interfaces import (
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    Indexer,
    IndexerConfig,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)

IndexerT = TypeVar("IndexerT", bound=Indexer)
IndexerConfigT = TypeVar("IndexerConfigT", bound=IndexerConfig)
DownloaderT = TypeVar("DownloaderT", bound=Downloader)
DownloaderConfigT = TypeVar("DownloaderConfigT", bound=DownloaderConfig)
ConnectionConfigT = TypeVar("ConnectionConfigT", bound=ConnectionConfig)
UploadStagerConfigT = TypeVar("UploadStagerConfigT", bound=UploadStagerConfig)
UploadStagerT = TypeVar("UploadStagerT", bound=UploadStager)
UploaderConfigT = TypeVar("UploaderConfigT", bound=UploaderConfig)
UploaderT = TypeVar("UploaderT", bound=Uploader)


class LocationShape(str, Enum):
    # Shape of a connector's target location; drives scope identity and recursion semantics.
    FSSPEC_URL = "fsspec-url"
    SQL_TABLE = "sql-table"
    SEARCH_INDEX = "search-index"
    API_FOLDER = "api-folder"
    OTHER = "other"


@dataclass
class RegistryEntry(ABC):
    # Capability markers. Default None == unannotated: consumers fall back to their
    # own defaults rather than deriving, so annotating a connector is opt-in and an
    # unannotated one is never mistaken for an explicit fsspec declaration.
    # kw_only so they don't disturb the required positional fields on subclasses.
    location_shape: Optional[LocationShape] = field(default=None, kw_only=True)
    supports_recursion: bool = field(default=True, kw_only=True)


@dataclass
class SourceRegistryEntry(RegistryEntry):
    indexer: Type[IndexerT]
    downloader: Type[DownloaderT]

    downloader_config: Optional[Type[DownloaderConfigT]] = None
    indexer_config: Optional[Type[IndexerConfigT]] = None
    connection_config: Optional[Type[ConnectionConfigT]] = None

    # Ordered reshaped-settings dot-paths that identify the targeted location.
    location_identity: tuple[str, ...] = field(default=("indexer_config.remote_url",), kw_only=True)
    emits_record_version: bool = field(default=False, kw_only=True)


source_registry: dict[str, SourceRegistryEntry] = {}


def add_source_entry(source_type: str, entry: SourceRegistryEntry):
    if source_type in source_registry:
        raise ValueError(f"source {source_type} has already been registered")
    source_registry[source_type] = entry


@dataclass
class DestinationRegistryEntry(RegistryEntry):
    uploader: Type[UploaderT]
    upload_stager: Optional[Type[UploadStagerT]] = None

    upload_stager_config: Optional[Type[UploadStagerConfigT]] = None
    uploader_config: Optional[Type[UploaderConfigT]] = None

    connection_config: Optional[Type[ConnectionConfigT]] = None

    # Ordered reshaped-settings dot-paths that identify the targeted location.
    location_identity: tuple[str, ...] = field(
        default=("uploader_config.remote_url",), kw_only=True
    )
    emits_record_version: bool = field(default=False, kw_only=True)


destination_registry: dict[str, DestinationRegistryEntry] = {}


def add_destination_entry(destination_type: str, entry: DestinationRegistryEntry):
    if destination_type in destination_registry:
        raise ValueError(f"destination {destination_type} has already been registered")
    destination_registry[destination_type] = entry
