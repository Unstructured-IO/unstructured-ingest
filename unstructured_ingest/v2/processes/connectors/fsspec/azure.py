from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generator, Optional

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
from unstructured_ingest.v2.processes.connectors.fsspec.utils import json_serial, sterilize_dict

CONNECTOR_TYPE = "azure"


def azure_json_serial(obj):
    from azure.storage.blob._models import ContentSettings

    if isinstance(obj, ContentSettings):
        return dict(obj)
    if isinstance(obj, bytearray):
        return str(obj)
    return json_serial(obj)


class AzureIndexerConfig(FsspecIndexerConfig):
    pass


class AzureAccessConfig(FsspecAccessConfig):
    account_name: Optional[str] = Field(
        default=None,
        description="The storage account name. This is used to authenticate "
        "requests signed with an account key and to construct "
        "the storage endpoint. It is required unless a connection "
        "string is given, or if a custom domain is used with "
        "anonymous authentication.",
    )
    account_key: Optional[str] = Field(
        default=None,
        description="The storage account key. This is used for shared key "
        "authentication. If any of account key, sas token or "
        "client_id are not specified, anonymous access will be used.",
    )
    connection_string: Optional[str] = Field(
        default=None,
        description="If specified, this will override all other parameters. See "
        "http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/ "  # noqa: E501
        "for the connection string format.",
    )
    sas_token: Optional[str] = Field(
        default=None,
        description="A shared access signature token to use to authenticate "
        "requests instead of the account key. If account key and "
        "sas token are both specified, account key will be used "
        "to sign. If any of account key, sas token or client_id "
        "are not specified, anonymous access will be used.",
    )

    def model_post_init(self, __context: Any) -> None:
        if self.connection_string is None and self.account_name is None:
            raise ValueError("either connection_string or account_name must be set")


SecretAzureAccessConfig = Secret[AzureAccessConfig]


class AzureConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = field(default_factory=lambda: ["az"], init=False)
    access_config: SecretAzureAccessConfig = Field(
        default_factory=lambda: SecretAzureAccessConfig(secret_value=AzureAccessConfig())
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    def get_access_config(self) -> dict[str, Any]:
        # Avoid injecting None by filtering out k,v pairs where the value is None
        access_configs: dict[str, Any] = {
            k: v for k, v in self.access_config.get_secret_value().dict().items() if v
        }
        return access_configs


@dataclass
class AzureIndexer(FsspecIndexer):
    connection_config: AzureConnectionConfig
    index_config: AzureIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["adlfs", "fsspec"], extras="azure")
    def precheck(self) -> None:
        super().precheck()

    def sterilize_info(self, path) -> dict:
        info = self.fs.info(path=path)
        return sterilize_dict(data=info, default=azure_json_serial)

    @requires_dependencies(["adlfs", "fsspec"], extras="azure")
    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        return super().run(**kwargs)


class AzureDownloaderConfig(FsspecDownloaderConfig):
    pass


@dataclass
class AzureDownloader(FsspecDownloader):
    protocol: str = "az"
    connection_config: AzureConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[AzureDownloaderConfig] = field(default_factory=AzureDownloaderConfig)

    @requires_dependencies(["adlfs", "fsspec"], extras="azure")
    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return super().run(file_data=file_data, **kwargs)

    @requires_dependencies(["adlfs", "fsspec"], extras="azure")
    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        return await super().run_async(file_data=file_data, **kwargs)


class AzureUploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class AzureUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: AzureConnectionConfig
    upload_config: AzureUploaderConfig = field(default=None)

    @requires_dependencies(["adlfs", "fsspec"], extras="azure")
    def __post_init__(self):
        super().__post_init__()

    @requires_dependencies(["adlfs", "fsspec"], extras="azure")
    def precheck(self) -> None:
        super().precheck()

    @requires_dependencies(["adlfs", "fsspec"], extras="azure")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        return super().run(path=path, file_data=file_data, **kwargs)

    @requires_dependencies(["adlfs", "fsspec"], extras="azure")
    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        return await super().run_async(path=path, file_data=file_data, **kwargs)


azure_source_entry = SourceRegistryEntry(
    indexer=AzureIndexer,
    indexer_config=AzureIndexerConfig,
    downloader=AzureDownloader,
    downloader_config=AzureDownloaderConfig,
    connection_config=AzureConnectionConfig,
)

azure_destination_entry = DestinationRegistryEntry(
    uploader=AzureUploader,
    uploader_config=AzureUploaderConfig,
    connection_config=AzureConnectionConfig,
)
