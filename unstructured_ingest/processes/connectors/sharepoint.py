from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncIterator, Optional

from pydantic import Field

from unstructured_ingest.data_types.file_data import (
    FileData,
)
from unstructured_ingest.error import (
    NotFoundError,
    SourceConnectionError,
    SourceConnectionNetworkError,
    UserAuthError,
    UserError,
    ValueError,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    SourceRegistryEntry,
)
from unstructured_ingest.processes.connectors.onedrive import (
    OnedriveAccessConfig,
    OnedriveConnectionConfig,
    OnedriveDownloader,
    OnedriveDownloaderConfig,
    OnedriveIndexer,
    OnedriveIndexerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from office365.onedrive.driveitems.driveItem import DriveItem
    from office365.onedrive.sites.site import Site
    from office365.runtime.client_request_exception import ClientRequestException

CONNECTOR_TYPE = "sharepoint"
LEGACY_DEFAULT_PATH = "Shared Documents"


class SharepointAccessConfig(OnedriveAccessConfig):
    client_cred: str = Field(description="Microsoft App client secret")


class SharepointConnectionConfig(OnedriveConnectionConfig):
    user_pname: Optional[str] = Field(
        default=None,
        description="User principal name or service account, usually your Azure AD email.",
    )
    site: str = Field(
        description="Sharepoint site url. Process either base url e.g \
                    https://[tenant].sharepoint.com  or relative sites \
                    https://[tenant].sharepoint.com/sites/<site_name>. \
                    To process all sites within the tenant pass a site url as \
                    https://[tenant]-admin.sharepoint.com.\
                    This requires the app to be registered at a tenant level"
    )
    library: Optional[str] = Field(
        default=None,
        description="Sharepoint library name. If not provided, the default \
                    drive will be used.",
    )

    def _get_drive_item(self, client_site: Site) -> DriveItem:
        """Helper method to get the drive item for the specified library or default drive."""
        site_drive_item = None
        if self.library:
            for drive in client_site.drives.get().execute_query():
                if drive.name == self.library:
                    logger.info(f"Found the requested library: {self.library}")
                    site_drive_item = drive.get().execute_query().root
                    break

        # If no specific library was found or requested, use the default drive
        if not site_drive_item:
            if self.library:
                logger.warning(
                    f"Library '{self.library}' not found in site '{self.site}'. "
                    "Using the default drive instead."
                )

            site_drive_item = client_site.drive.get().execute_query().root

        return site_drive_item


class SharepointIndexerConfig(OnedriveIndexerConfig):
    # TODO: We can probably make path non-optional on OnedriveIndexerConfig once tested
    path: str = Field(default="")


@dataclass
class SharepointIndexer(OnedriveIndexer):
    connection_config: SharepointConnectionConfig
    index_config: SharepointIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def _handle_client_request_exception(self, e: ClientRequestException, context: str) -> None:
        """Convert ClientRequestException to appropriate user-facing error based on HTTP status."""
        if hasattr(e, "response") and e.response is not None and hasattr(e.response, "status_code"):
            status_code = e.response.status_code
            if status_code == 401:
                raise UserAuthError(
                    f"Unauthorized access to {context}. Check client credentials and permissions"
                )
            elif status_code == 403:
                raise UserAuthError(
                    f"Access forbidden to {context}. "
                    f"Check app permissions (Sites.Read.All required)"
                )
            elif status_code == 404:
                raise UserError(f"Not found: {context}")

        raise UserError(f"Failed to access {context}: {str(e)}")

    def _is_root_path(self, path: str) -> bool:
        """Check if the path represents root access (empty string or legacy default)."""
        return not path or not path.strip() or path == LEGACY_DEFAULT_PATH

    def _get_target_drive_item(self, site_drive_item: DriveItem, path: str) -> DriveItem:
        """Get the drive item to search in based on the path."""
        if self._is_root_path(path):
            return site_drive_item
        else:
            return site_drive_item.get_by_path(path).get().execute_query()

    def _validate_folder_path(self, site_drive_item: DriveItem, path: str) -> None:
        """Validate that a specific folder path exists and is accessible."""
        from office365.runtime.client_request_exception import ClientRequestException

        try:
            path_item = site_drive_item.get_by_path(path).get().execute_query()
            if path_item is None or not hasattr(path_item, "is_folder"):
                raise UserError(
                    f"SharePoint path '{path}' not found in site {self.connection_config.site}. "
                    f"Check that the path exists and you have access to it"
                )
            logger.info(f"SharePoint folder path '{path}' validated successfully")
        except ClientRequestException as e:
            logger.error(f"Failed to access SharePoint path '{path}': {e}")
            self._handle_client_request_exception(e, f"SharePoint path '{path}'")
        except Exception as e:
            logger.error(f"Unexpected error accessing SharePoint path '{path}': {e}")
            raise UserError(f"Failed to validate SharePoint path '{path}': {str(e)}")

    @requires_dependencies(["office365"], extras="sharepoint")
    def precheck(self) -> None:
        """Validate SharePoint connection before indexing."""
        from office365.runtime.client_request_exception import ClientRequestException

        # Validate authentication - this call will raise UserAuthError if invalid
        self.connection_config.get_token()

        try:
            client = self.connection_config.get_client()
            client_site = client.sites.get_by_url(self.connection_config.site).get().execute_query()
            site_drive_item = self.connection_config._get_drive_item(client_site)

            path = self.index_config.path
            if not self._is_root_path(path):
                self._validate_folder_path(site_drive_item, path)

            logger.info(
                f"SharePoint connection validated successfully for site: "
                f"{self.connection_config.site}"
            )

        except ClientRequestException as e:
            logger.error(f"SharePoint precheck failed for site: {self.connection_config.site}")
            self._handle_client_request_exception(
                e, f"SharePoint site {self.connection_config.site}"
            )
        except Exception as e:
            logger.error(f"Unexpected error during SharePoint precheck: {e}", exc_info=True)
            raise UserError(f"Failed to validate SharePoint connection: {str(e)}")

    @requires_dependencies(["office365"], extras="sharepoint")
    async def run_async(self, **kwargs: Any) -> AsyncIterator[FileData]:
        from office365.runtime.client_request_exception import ClientRequestException

        token_resp = await asyncio.to_thread(self.connection_config.get_token)
        if "error" in token_resp:
            raise SourceConnectionError(
                f"[{self.connector_type}]: {token_resp['error']} "
                f"({token_resp.get('error_description')})"
            )

        client = await asyncio.to_thread(self.connection_config.get_client)
        try:
            client_site = client.sites.get_by_url(self.connection_config.site).get().execute_query()
            site_drive_item = self.connection_config._get_drive_item(client_site)
        except ClientRequestException as e:
            logger.error(f"Failed to access SharePoint site: {self.connection_config.site}")
            raise SourceConnectionError(
                f"Unable to access SharePoint site at {self.connection_config.site}: {str(e)}"
            )

        path = self.index_config.path
        target_drive_item = await asyncio.to_thread(
            self._get_target_drive_item, site_drive_item, path
        )

        for drive_item in target_drive_item.get_files(
            recursive=self.index_config.recursive
        ).execute_query():
            file_data = await self.drive_item_to_file_data(drive_item=drive_item)
            yield file_data


class SharepointDownloaderConfig(OnedriveDownloaderConfig):
    max_retries: int = 10


@dataclass
class SharepointDownloader(OnedriveDownloader):
    connection_config: SharepointConnectionConfig
    download_config: SharepointDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @staticmethod
    def retry_on_status_code(exc):
        error_msg = str(exc).lower()
        return "429" in error_msg or "activitylimitreached" in error_msg or "throttled" in error_msg

    @SourceConnectionNetworkError.wrap
    @requires_dependencies(["office365"], extras="sharepoint")
    def _fetch_file(self, file_data: FileData) -> DriveItem:
        from office365.runtime.client_request_exception import ClientRequestException
        from tenacity import (
            before_log,
            retry,
            retry_if_exception,
            stop_after_attempt,
            wait_exponential,
        )

        if file_data.source_identifiers is None or not file_data.source_identifiers.fullpath:
            raise ValueError(
                f"file data doesn't have enough information to get "
                f"file content: {file_data.model_dump()}"
            )

        server_relative_path = file_data.source_identifiers.fullpath
        client = self.connection_config.get_client()

        @retry(
            stop=stop_after_attempt(self.download_config.max_retries),
            wait=wait_exponential(exp_base=2, multiplier=1, min=2, max=10),
            retry=retry_if_exception(self.retry_on_status_code),
            before=before_log(logger, logging.DEBUG),
            reraise=True,
        )
        def _get_item_by_path() -> DriveItem:
            try:
                client_site = (
                    client.sites.get_by_url(self.connection_config.site).get().execute_query()
                )
                site_drive_item = self.connection_config._get_drive_item(client_site)
            except ClientRequestException:
                logger.info(f"Site not found: {self.connection_config.site}")
                raise SourceConnectionError(f"Site not found: {self.connection_config.site}")
            file = site_drive_item.get_by_path(server_relative_path).get().execute_query()
            return file

        # Call the retry-wrapped function
        file = _get_item_by_path()

        if not file:
            raise NotFoundError(f"file not found: {server_relative_path}")
        return file


sharepoint_source_entry = SourceRegistryEntry(
    connection_config=SharepointConnectionConfig,
    indexer_config=SharepointIndexerConfig,
    indexer=SharepointIndexer,
    downloader_config=SharepointDownloaderConfig,
    downloader=SharepointDownloader,
)
