from __future__ import annotations

from dataclasses import dataclass, field
from time import time
from typing import TYPE_CHECKING, Any, Optional

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
    pass

CONNECTOR_TYPE = "dropbox"


class DropboxIndexerConfig(FsspecIndexerConfig):
    def model_post_init(self, __context):
        if not self.path_without_protocol.startswith("/"):
            self.path_without_protocol = "/" + self.path_without_protocol


class DropboxAccessConfig(FsspecAccessConfig):
    token: Optional[str] = Field(
        default=None, description="Dropbox access token."
    )  # This is the short lived (4h) token that needs to be generated anew each time.
    app_key: Optional[str] = Field(default=None, description="Dropbox app key.")
    app_secret: Optional[str] = Field(default=None, description="Dropbox app secret.")
    refresh_token: Optional[str] = Field(
        default=None, description="Dropbox refresh token."
    )  # This is the long lived token that doesn't expire


class DropboxConnectionConfig(FsspecConnectionConfig):
    access_config: Secret[DropboxAccessConfig] = Field(
        default=DropboxAccessConfig(), validate_default=True
    )
    connector_type: str = Field(default=CONNECTOR_TYPE)

    @requires_dependencies(["dropbox"])
    def get_dropbox_access_token_from_refresh(
        self,
        refresh_token: str,
        app_key: str,
        app_secret: str,
    ) -> str:
        """
        Uses the Dropbox Python SDK to exchange a long-lived refresh token for an access token.
        """
        import dropbox

        dbx = dropbox.Dropbox(
            oauth2_access_token=None,
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )

        # This call fetches a new short-lived token and auto-updates dbx._oauth2_access_token
        dbx.check_and_refresh_access_token()
        short_lived_token = dbx._oauth2_access_token  # Private attr, but standard usage
        return short_lived_token

    def get_access_config(self) -> dict[str, Any]:
        """
        Overrides the parent FsspecConnectionConfig.get_access_config() to ensure
        that we always provide an access token if refresh credentials exist.
        """
        base_conf = super().get_access_config()

        refresh_token = base_conf.get("refresh_token")
        app_key = base_conf.get("app_key")
        app_secret = base_conf.get("app_secret")

        # Standard scenario - we have refresh a token and creds provided
        # which we're going to use to retrieve access token
        if refresh_token and app_key and app_secret:
            logger.debug("Attempting to generate access token from refresh token...")
            new_token = self.get_dropbox_access_token_from_refresh(
                refresh_token=refresh_token,
                app_key=app_key,
                app_secret=app_secret,
            )
            if not new_token:
                raise ValueError(
                    "Unable to retrieve an access token from Dropbox. "
                    "Please check that your refresh token, app key, and secret are valid."
                )
            base_conf["token"] = new_token
        elif not base_conf.get("token"):  # we might already have an access token from outside
            # We have neither an existing short?lived token nor refresh credentials
            raise ValueError(
                "No valid token or refresh_token with app credentials was found. "
                "Please check that your refresh token, app key, and secret are valid "
                "or provide a valid short-lived token"
            )

        return base_conf

    @requires_dependencies(["dropbox"])
    def wrap_error(self, e: Exception) -> Exception:
        from dropbox.exceptions import AuthError, HttpError, RateLimitError

        if not isinstance(e, HttpError):
            logger.error(f"Unhandled Dropbox exception: {repr(e)}", exc_info=True)
            return e

        if isinstance(e, AuthError):
            raise UserAuthError(e.error)
        elif isinstance(e, RateLimitError):
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

        logger.error(f"Unhandled Dropbox HttpError: {repr(e)}", exc_info=True)
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
