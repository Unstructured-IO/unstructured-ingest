from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from time import time
from typing import TYPE_CHECKING, Annotated, Any, Generator, Optional

from dateutil import parser
from pydantic import Field, Secret
from pydantic.functional_validators import BeforeValidator

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
from unstructured_ingest.processes.connectors.utils import conform_string_to_dict
from unstructured_ingest.processes.utils.blob_storage import (
    BlobStoreUploadStager,
    BlobStoreUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from boxfs import BoxFileSystem

CONNECTOR_TYPE = "box"


class BoxIndexerConfig(FsspecIndexerConfig):
    pass


class BoxAccessConfig(FsspecAccessConfig):
    box_app_config: Annotated[dict, BeforeValidator(conform_string_to_dict)] = Field(
        description="Box app credentials as a JSON string."
    )


class BoxConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = field(default_factory=lambda: ["box"], init=False)
    access_config: Secret[BoxAccessConfig]
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    def get_access_config(self) -> dict[str, Any]:
        from boxsdk import JWTAuth

        ac = self.access_config.get_secret_value()
        settings_dict = ac.box_app_config

        # Create and authenticate the JWTAuth object
        oauth = JWTAuth.from_settings_dictionary(settings_dict)
        oauth.authenticate_instance()

        # if not oauth.access_token:
        #     raise SourceConnectionError("Authentication failed: No access token generated.")

        # Prepare the access configuration with the authenticated oauth
        access_kwargs_with_oauth: dict[str, Any] = {
            "oauth": oauth,
        }
        access_config: dict[str, Any] = ac.model_dump()
        access_config.pop("box_app_config", None)
        access_kwargs_with_oauth.update(access_config)

        return access_kwargs_with_oauth

    def wrap_error(self, e: Exception) -> Exception:
        from boxsdk.exception import BoxAPIException, BoxOAuthException

        if isinstance(e, BoxOAuthException):
            return UserAuthError(e.message)
        if not isinstance(e, BoxAPIException):
            logger.error(f"unhandled exception from box ({type(e)}): {e}", exc_info=True)
            return e
        message = e.message or e
        if error_code_status := e.status:
            if 400 <= error_code_status < 500:
                return UserError(message)
            if error_code_status >= 500:
                return ProviderError(message)

        logger.error(f"unhandled exception from box ({type(e)}): {e}", exc_info=True)
        return e

    @requires_dependencies(["boxfs"], extras="box")
    @contextmanager
    def get_client(self, protocol: str) -> Generator["BoxFileSystem", None, None]:
        with super().get_client(protocol=protocol) as client:
            yield client


@dataclass
class BoxIndexer(FsspecIndexer):
    connection_config: BoxConnectionConfig
    index_config: BoxIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def get_metadata(self, file_info: dict) -> FileDataSourceMetadata:
        path = file_info["name"]
        date_created = None
        date_modified = None
        if modified_at_str := file_info.get("modified_at"):
            date_modified = str(parser.parse(modified_at_str).timestamp())
        if created_at_str := file_info.get("created_at"):
            date_created = str(parser.parse(created_at_str).timestamp())

        file_size = file_info.get("size") if "size" in file_info else None

        version = file_info.get("id")
        record_locator = {
            "protocol": self.index_config.protocol,
            "remote_file_path": self.index_config.remote_url,
            "file_id": file_info.get("id"),
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


class BoxUploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class BoxUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: BoxConnectionConfig
    upload_config: BoxUploaderConfig = field(default=None)


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
    upload_stager_config=BlobStoreUploadStagerConfig,
    upload_stager=BlobStoreUploadStager,
)
