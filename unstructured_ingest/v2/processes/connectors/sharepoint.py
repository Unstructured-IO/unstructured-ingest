from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, AsyncIterator, Optional

from dateutil import parser
from pydantic import Field, Secret

from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)

if TYPE_CHECKING:
    from office365.graph_client import GraphClient
    from office365.onedrive.driveitems.driveItem import DriveItem
    from office365.onedrive.drives.drive import Drive

CONNECTOR_TYPE = "sharepoint"
MAX_BYTES_SIZE = 512_000_000


class SharepointAccessConfig(AccessConfig):
    client_cred: str = Field(description="Microsoft App client secret")


class SharepointConnectionConfig(ConnectionConfig):
    client_id: str = Field(description="Microsoft app client ID")
    site: str = Field(
        description="Sharepoint site url. Process either base url e.g \
                    https://[tenant].sharepoint.com  or relative sites \
                    https://[tenant].sharepoint.com/sites/<site_name>. \
                    To process all sites within the tenant pass a site url as \
                    https://[tenant]-admin.sharepoint.com.\
                    This requires the app to be registered at a tenant level"
    )
    user_pname: str = Field(description="User principal name, usually is your Azure AD email.")
    tenant: str = Field(
        repr=False, description="ID or domain name associated with your Azure AD instance"
    )
    authority_url: Optional[str] = Field(
        repr=False,
        default="https://login.microsoftonline.com",
        examples=["https://login.microsoftonline.com"],
        description="Authentication token provider for Microsoft apps",
    )
    access_config: Secret[SharepointAccessConfig]

    def get_drive(self) -> "Drive":
        client = self.get_client()
        drive = client.users[self.user_pname].drive
        return drive

    @requires_dependencies(["msal"], extras="sharepoint")
    def get_token(self):
        from msal import ConfidentialClientApplication

        try:
            app = ConfidentialClientApplication(
                authority=f"{self.authority_url}/{self.tenant}",
                client_id=self.client_id,
                client_credential=self.access_config.get_secret_value().client_cred,
            )
            token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        except ValueError as exc:
            logger.error("Couldn't set up credentials for Sharepoint")
            raise exc
        if "error" in token:
            raise SourceConnectionNetworkError(
                "failed to fetch token, {}: {}".format(token["error"], token["error_description"])
            )
        return token

    @requires_dependencies(["office365"], extras="sharepoint")
    def get_client(self) -> "GraphClient":
        from office365.graph_client import GraphClient

        client = GraphClient(self.get_token)
        return client


class SharepointIndexerConfig(IndexerConfig):
    path: Optional[str] = Field(default="")
    recursive: bool = False


@dataclass
class SharepointIndexer(Indexer):
    connection_config: SharepointConnectionConfig
    index_config: SharepointIndexerConfig

    def precheck(self) -> None:
        try:
            token_resp: dict = self.connection_config.get_token()
            if error := token_resp.get("error"):
                raise SourceConnectionError(
                    "{} ({})".format(error, token_resp.get("error_description"))
                )
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def list_objects_sync(self, folder: DriveItem, recursive: bool) -> list["DriveItem"]:
        drive_items = folder.children.get().execute_query()
        files = [d for d in drive_items if d.is_file]
        if not recursive:
            return files

        folders = [d for d in drive_items if d.is_folder]
        for f in folders:
            files.extend(self.list_objects_sync(f, recursive))
        return files

    async def list_objects(self, folder: "DriveItem", recursive: bool) -> list["DriveItem"]:
        return await asyncio.to_thread(self.list_objects_sync, folder, recursive)

    def get_root_sync(self, client: "GraphClient") -> "DriveItem":
        root = client.users[self.connection_config.user_pname].drive.get().execute_query().root
        if fpath := self.index_config.path:
            root = root.get_by_path(fpath).get().execute_query()
            if root is None or not root.is_folder:
                raise ValueError(f"Unable to find directory, given: {fpath}")
        return root

    async def get_root(self, client: "GraphClient") -> "DriveItem":
        return await asyncio.to_thread(self.get_root_sync, client)

    def get_properties_sync(self, drive_item: "DriveItem") -> dict:
        properties = drive_item.properties
        filtered_properties = {}
        for k, v in properties.items():
            try:
                json.dumps(v)
                filtered_properties[k] = v
            except TypeError:
                pass
        return filtered_properties

    async def get_properties(self, drive_item: "DriveItem") -> dict:
        return await asyncio.to_thread(self.get_properties_sync, drive_item)

    def drive_item_to_file_data_sync(self, drive_item: "DriveItem") -> FileData:
        file_path = drive_item.parent_reference.path.split(":")[-1]
        file_path = file_path[1:] if file_path and file_path[0] == "/" else file_path
        filename = drive_item.name
        server_path = file_path + "/" + filename
        rel_path = server_path.replace(self.index_config.path, "").lstrip("/")
        date_modified_dt = (
            parser.parse(str(drive_item.last_modified_datetime))
            if drive_item.last_modified_datetime
            else None
        )
        date_created_at = (
            parser.parse(str(drive_item.created_datetime)) if drive_item.created_datetime else None
        )
        return FileData(
            identifier=drive_item.id,
            connector_type=CONNECTOR_TYPE,
            source_identifiers=SourceIdentifiers(
                fullpath=server_path, filename=drive_item.name, rel_path=rel_path
            ),
            metadata=FileDataSourceMetadata(
                url=drive_item.parent_reference.path + "/" + drive_item.name,
                version=drive_item.etag,
                date_modified=str(date_modified_dt.timestamp()) if date_modified_dt else None,
                date_created=str(date_created_at.timestamp()) if date_created_at else None,
                date_processed=str(time()),
                record_locator={
                    "user_pname": self.connection_config.user_pname,
                    "server_relative_path": server_path,
                },
            ),
            additional_metadata=self.get_properties_sync(drive_item=drive_item),
        )

    async def drive_item_to_file_data(self, drive_item: "DriveItem") -> FileData:
        # Offload the file data creation if it's not guaranteed async
        return await asyncio.to_thread(self.drive_item_to_file_data_sync, drive_item)

    def is_async(self) -> bool:
        return True
    
    @requires_dependencies(["office365"], extras="onedrive")
    async def run_async(self, **kwargs: Any) -> AsyncIterator[FileData]:
        from office365.runtime.client_request_exception import ClientRequestException
        token_resp = await asyncio.to_thread(self.connection_config.get_token)
        if "error" in token_resp:
            raise SourceConnectionError(
                f"[{CONNECTOR_TYPE}]: {token_resp['error']} ({token_resp.get('error_description')})"
            )

        client = await asyncio.to_thread(self.connection_config.get_client)
        # root = await self.get_root(client=client)
        # drive_items = await self.list_objects(folder=root, recursive=self.index_config.recursive)
        try:
            site= client.sites.get_by_url(self.connection_config.site).get().execute_query()
            site_drive_item = site.drive.get().execute_query().root
        except ClientRequestException:
            logger.info("Site not found")
        
        drive_items = await self.list_objects(folder=site_drive_item, recursive=self.index_config.recursive)
        for drive_item in drive_items:
            file_data = await self.drive_item_to_file_data(drive_item=drive_item)
            yield file_data


class SharepointDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class SharepointDownloader(Downloader):
    connection_config: SharepointConnectionConfig
    download_config: SharepointDownloaderConfig

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
        # root = client.users[self.connection_config.user_pname].drive.get().execute_query().root
        # file = root.get_by_path(server_relative_path).get().execute_query()

        try:
            site= client.sites.get_by_url(self.connection_config.site).get().execute_query()
            site_drive_item = site.drive.get().execute_query().root
        except ClientRequestException:
            logger.info("Site not found")
        file = site_drive_item.get_by_path(server_relative_path).get().execute_query()

        if not file:
            raise FileNotFoundError(f"file not found: {server_relative_path}")
        return file

    def get_download_path(self, file_data: FileData) -> Optional[Path]:
        rel_path = file_data.source_identifiers.relative_path
        rel_path = rel_path[1:] if rel_path.startswith("/") else rel_path
        return self.download_dir / Path(rel_path)

    @SourceConnectionError.wrap
    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        try:
            file = self._fetch_file(file_data=file_data)
            fsize = file.get_property("size", 0)
            download_path = self.get_download_path(file_data=file_data)
            download_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"downloading {file_data.source_identifiers.fullpath} to {download_path}")
            if fsize > MAX_BYTES_SIZE:
                logger.info(f"downloading file with size: {fsize} bytes in chunks")
                with download_path.open(mode="wb") as f:
                    file.download_session(f, chunk_size=1024 * 1024 * 100).execute_query()
            else:
                with download_path.open(mode="wb") as f:
                    file.download_session(f).execute_query()
            return self.generate_download_response(file_data=file_data, download_path=download_path)
        except Exception as e:
            logger.error(f"[{CONNECTOR_TYPE}] Exception during downloading: {e}", exc_info=True)
            # Re-raise to see full stack trace locally
            raise


sharepoint_source_entry = SourceRegistryEntry(
    connection_config=SharepointConnectionConfig,
    indexer_config=SharepointIndexerConfig,
    indexer=SharepointIndexer,
    downloader_config=SharepointDownloaderConfig,
    downloader=SharepointDownloader,
)
