from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileDataSourceMetadata
from unstructured_ingest.errors_v2 import ProviderError, UserAuthError, UserError
from unstructured_ingest.logger import logger
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
from unstructured_ingest.processes.connectors.fsspec.utils import json_serial, sterilize_dict
from unstructured_ingest.processes.utils.blob_storage import (
    BlobStoreUploadStager,
    BlobStoreUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from adlfs import AzureBlobFileSystem

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


class AzureConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = field(default_factory=lambda: ["az"], init=False)
    access_config: Secret[AzureAccessConfig]
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    def get_access_config(self) -> dict[str, Any]:
        # Avoid injecting None by filtering out k,v pairs where the value is None
        access_configs: dict[str, Any] = {
            k: v for k, v in self.access_config.get_secret_value().model_dump().items() if v
        }
        return access_configs

    @requires_dependencies(["adlfs", "fsspec"], extras="azure")
    @contextmanager
    def get_client(self, protocol: str) -> Generator["AzureBlobFileSystem", None, None]:
        with super().get_client(protocol=protocol) as client:
            yield client

    def wrap_error(self, e: Exception) -> Exception:
        from azure.core.exceptions import ClientAuthenticationError, HttpResponseError

        if not isinstance(e, HttpResponseError):
            logger.error(f"unhandled exception from azure ({type(e)}): {e}", exc_info=True)
            return e
        if isinstance(e, ClientAuthenticationError):
            return UserAuthError(e.reason)
        status_code = e.status_code
        message = e.reason
        if status_code is not None:
            if 400 <= status_code < 500:
                return UserError(message)
            if status_code >= 500:
                return ProviderError(message)
        logger.error(f"unhandled exception from azure ({type(e)}): {e}", exc_info=True)
        return e


@dataclass
class AzureIndexer(FsspecIndexer):
    connection_config: AzureConnectionConfig
    index_config: AzureIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def sterilize_info(self, file_data: dict) -> dict:
        return sterilize_dict(data=file_data, default=azure_json_serial)

    def get_metadata(self, file_info: dict) -> FileDataSourceMetadata:
        path = file_info["name"]
        date_created = (
            str(file_info.get("creation_time").timestamp())
            if "creation_time" in file_info
            else None
        )
        date_modified = (
            str(file_info.get("last_modified").timestamp())
            if "last_modified" in file_info
            else None
        )

        file_size = file_info.get("size") if "size" in file_info else None

        version = file_info.get("etag")
        record_locator = {
            "protocol": self.index_config.protocol,
            "remote_file_path": self.index_config.remote_url,
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


class AzureDownloaderConfig(FsspecDownloaderConfig):
    pass


@dataclass
class AzureDownloader(FsspecDownloader):
    protocol: str = "az"
    connection_config: AzureConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[AzureDownloaderConfig] = field(default_factory=AzureDownloaderConfig)


class AzureUploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class AzureUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: AzureConnectionConfig
    upload_config: AzureUploaderConfig = field(default=None)


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
    upload_stager_config=BlobStoreUploadStagerConfig,
    upload_stager=BlobStoreUploadStager,
)
