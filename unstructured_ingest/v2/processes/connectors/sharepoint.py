from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncIterator

from pydantic import Field

from unstructured_ingest.error import (
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    FileData,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.onedrive import (
    OnedriveAccessConfig,
    OnedriveConnectionConfig,
    OnedriveDownloader,
    OnedriveDownloaderConfig,
    OnedriveIndexer,
    OnedriveIndexerConfig,
)

if TYPE_CHECKING:
    from office365.onedrive.driveitems.driveItem import DriveItem

CONNECTOR_TYPE = "sharepoint"
LEGACY_DEFAULT_PATH = "Shared Documents"


class SharepointAccessConfig(OnedriveAccessConfig):
    client_cred: str = Field(description="Microsoft App client secret")


class SharepointConnectionConfig(OnedriveConnectionConfig):
    site: str = Field(
        description="Sharepoint site url. Process either base url e.g \
                    https://[tenant].sharepoint.com  or relative sites \
                    https://[tenant].sharepoint.com/sites/<site_name>. \
                    To process all sites within the tenant pass a site url as \
                    https://[tenant]-admin.sharepoint.com.\
                    This requires the app to be registered at a tenant level"
    )


class SharepointIndexerConfig(OnedriveIndexerConfig):
    pass


@dataclass
class SharepointIndexer(OnedriveIndexer):
    connection_config: SharepointConnectionConfig
    index_config: SharepointIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["office365"], extras="sharepoint")
    async def run_async(self, **kwargs: Any) -> AsyncIterator[FileData]:
        from office365.runtime.client_request_exception import ClientRequestException

        logger.info(f"[{self.connector_type}] Fetching access token...")
        token_resp = await asyncio.to_thread(self.connection_config.get_token)
    
        if "error" in token_resp:
            error_message = f"[{self.connector_type}] Authentication error: {token_resp['error']} \
            ({token_resp.get('error_description')})"
            logger.error(error_message)
            raise SourceConnectionError(error_message)

        logger.info(f"[{self.connector_type}] Successfully obtained access token. \
        Connecting to SharePoint site: {self.connection_config.site}")

        client = await asyncio.to_thread(self.connection_config.get_client)

        try:
            site = client.sites.get_by_url(self.connection_config.site).get().execute_query()
            logger.info(f"[{self.connector_type}] Successfully retrieved site object: {site.url}")
        
            site_drive_item = site.drive.get().execute_query().root
            if site_drive_item is None:
                raise ValueError(f"[{self.connector_type}] No root drive found for site {self.connection_config.site}. \
                Please check site permissions or if the site has a document library.")
        
            logger.info(f"[{self.connector_type}] Successfully retrieved site drive root.")
        
        except ClientRequestException as e:
            logger.error(f"[{self.connector_type}] Failed to fetch SharePoint site: {str(e)}")
            raise SourceConnectionError(f"[{self.connector_type}] Site not found or inaccessible: {str(e)}")
    
        # Check if a path was provided and attempt to retrieve the specific path
        path = self.index_config.path
        if path and path != LEGACY_DEFAULT_PATH:
            logger.info(f"[{self.connector_type}] Fetching site drive item at path: {path}")

            try:
                site_drive_item = site_drive_item.get_by_path(path).get().execute_query()
                logger.info(f"[{self.connector_type}] Successfully retrieved site drive item at path: {path}")
            except ClientRequestException as e:
                logger.error(f"[{self.connector_type}] Invalid path '{path}'. \
                Please verify the path exists in SharePoint. Error: {str(e)}")
                raise ValueError(f"[{self.connector_type}] Invalid path '{path}' or path does not exist.")

        # Final validation before proceeding to file retrieval
        if site_drive_item is None:
            error_msg = f"[{self.connector_type}] Unable to retrieve site drive item. \
            This may be due to incorrect site URL, missing permissions, or an empty document library."
            logger.error(error_msg)
            raise ValueError(error_msg)

        for drive_item in site_drive_item.get_files(
            recursive=self.index_config.recursive
        ).execute_query():
            file_data = await self.drive_item_to_file_data(drive_item=drive_item)
            yield file_data



class SharepointDownloaderConfig(OnedriveDownloaderConfig):
    pass


@dataclass
class SharepointDownloader(OnedriveDownloader):
    connection_config: SharepointConnectionConfig
    download_config: SharepointDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @SourceConnectionNetworkError.wrap
    @requires_dependencies(["office365"], extras="onedrive")
    def _fetch_file(self, file_data: FileData) -> DriveItem:
        from office365.runtime.client_request_exception import ClientRequestException

        if file_data.source_identifiers is None or not file_data.source_identifiers.fullpath:
            raise ValueError(
                f"file data doesn't have enough information to get "
                f"file content: {file_data.model_dump()}"
            )

        server_relative_path = file_data.source_identifiers.fullpath
        client = self.connection_config.get_client()

        try:
            site = client.sites.get_by_url(self.connection_config.site).get().execute_query()
            site_drive_item = site.drive.get().execute_query().root
        except ClientRequestException:
            logger.info("Site not found")
        file = site_drive_item.get_by_path(server_relative_path).get().execute_query()

        if not file:
            raise FileNotFoundError(f"file not found: {server_relative_path}")
        return file


sharepoint_source_entry = SourceRegistryEntry(
    connection_config=SharepointConnectionConfig,
    indexer_config=SharepointIndexerConfig,
    indexer=SharepointIndexer,
    downloader_config=SharepointDownloaderConfig,
    downloader=SharepointDownloader,
)
