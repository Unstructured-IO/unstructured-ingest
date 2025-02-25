from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from time import time
from typing import TYPE_CHECKING, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.errors import (
    ProviderError,
    UserAuthError,
    UserError,
)
from unstructured_ingest.v2.errors import (
    RateLimitError as CustomRateLimitError,
)
from unstructured_ingest.v2.interfaces import FileDataSourceMetadata
from unstructured_ingest.v2.logger import logger
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

if TYPE_CHECKING:
    from dropboxdrivefs import DropboxDriveFileSystem

CONNECTOR_TYPE = "dropbox"


class DropboxIndexerConfig(FsspecIndexerConfig):
    def model_post_init(self, __context):
        if not self.path_without_protocol.startswith("/"):
            self.path_without_protocol = "/" + self.path_without_protocol


class DropboxAccessConfig(FsspecAccessConfig):
    token: Optional[str] = Field(default=None, description="Dropbox access token.") #This is the short lived (4h) token that needs to be generated anew each time.
    app_key: Optional[str] = Field(default=None, description="Dropbox app key.")
    app_secret: Optional[str] = Field(default=None, description="Dropbox app secret.")
    refresh_token: Optional[str] = Field(default=None, description="Dropbox refresh token.") #This is the long lived token that doesn't expire


class DropboxConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = field(default_factory=lambda: ["dropbox"], init=False)
    access_config: Secret[DropboxAccessConfig] = Field(
        default=DropboxAccessConfig(), validate_default=True
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)


    @requires_dependencies(["dropbox"])
    def get_dropbox_access_token_from_refresh(
        refresh_token: str, 
        app_key: str, 
        app_secret: str
    ) -> str:
        import dropbox

        dbx = dropbox.Dropbox(
            oauth2_access_token=None,
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )

        dbx.check_and_refresh_access_token()

        # The Dropbox SDK will have fetched a new short-lived token under the hood
        short_lived_token = dbx._oauth2_access_token  # official attribute is private, but commonly used
        return short_lived_token

    @requires_dependencies(["dropboxdrivefs", "fsspec"], extras="dropbox")
    @contextmanager
    def get_client(self, protocol: str) -> Generator["DropboxDriveFileSystem", None, None]:
        from fsspec import get_filesystem_class

        """
        Create a DropboxDriveFileSystem client with a fresh short-lived token,
        if a refresh token is available. Otherwise, just fall back to a user-
        supplied short-lived token.
        """
        access_conf = self.get_access_config()  # -> dict[str, Any]

        short_lived_token = access_conf.get("token")
        refresh_token = access_conf.get("refresh_token")
        app_key = access_conf.get("app_key")
        app_secret = access_conf.get("app_secret")

        # If refresh creds are present, refresh the token
        if refresh_token and app_key and app_secret:
            short_lived_token = self.get_dropbox_access_token_from_refresh(
                refresh_token=refresh_token,
                app_key=app_key,
                app_secret=app_secret,
            )
            # Update our config dict with the fresh short-lived token.
            access_conf["token"] = short_lived_token

        # Hand off to fsspec using the short-lived token
        fs_class = get_filesystem_class(protocol)
        client = fs_class(**access_conf)
        try:
            yield client
        finally:
            # Not all fsspec filesystems require explicit close, 
            # but it's good practice to do so if it's available.
            close_method = getattr(client, "close", None)
            if callable(close_method):
                close_method()

    def wrap_error(self, e: Exception) -> Exception:
        from dropbox.exceptions import AuthError, HttpError, RateLimitError

        if not isinstance(e, HttpError):
            logger.error(f"unhandled exception from dropbox ({type(e)}): {e}", exc_info=True)
            return e
        if isinstance(e, AuthError):
            raise UserAuthError(e.error)
        if isinstance(e, RateLimitError):
            return CustomRateLimitError(e.error)
        status_code = e.status_code
        if 400 <= status_code < 500:
            if body := getattr(e, "body", None):
                return UserError(body)
            else:
                return UserError(e.body)
        if status_code >= 500:
            if body := getattr(e, "body", None):
                return ProviderError(body)
            else:
                return ProviderError(e.body)
        logger.error(f"unhandled exception from dropbox ({type(e)}): {e}", exc_info=True)
        return e


@dataclass
class DropboxIndexer(FsspecIndexer):
    connection_config: DropboxConnectionConfig
    index_config: DropboxIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def get_path(self, file_info: dict) -> str:
        return file_info["name"]

    def get_metadata(self, file_info: dict) -> FileDataSourceMetadata:
        path = file_info["name"].lstrip("/")
        date_created = None
        date_modified = None
        server_modified = file_info.get("server_modified")
        client_modified = file_info.get("client_modified")
        if server_modified and client_modified and server_modified > client_modified:
            date_created = str(client_modified.timestamp())
            date_modified = str(server_modified.timestamp())
        elif server_modified and client_modified and server_modified < client_modified:
            date_created = str(server_modified.timestamp())
            date_modified = str(client_modified.timestamp())

        file_size = file_info.get("size") if "size" in file_info else None

        version = file_info.get("content_hash")
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


class DropboxDownloaderConfig(FsspecDownloaderConfig):
    pass


@dataclass
class DropboxDownloader(FsspecDownloader):
    protocol: str = "dropbox"
    connection_config: DropboxConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[DropboxDownloaderConfig] = field(
        default_factory=DropboxDownloaderConfig
    )


class DropboxUploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class DropboxUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: DropboxConnectionConfig
    upload_config: DropboxUploaderConfig = field(default=None)


dropbox_source_entry = SourceRegistryEntry(
    indexer=DropboxIndexer,
    indexer_config=DropboxIndexerConfig,
    downloader=DropboxDownloader,
    downloader_config=DropboxDownloaderConfig,
    connection_config=DropboxConnectionConfig,
)

dropbox_destination_entry = DestinationRegistryEntry(
    uploader=DropboxUploader,
    uploader_config=DropboxUploaderConfig,
    connection_config=DropboxConnectionConfig,
)
