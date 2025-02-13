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

CONNECTOR_TYPE = "onedrive"
MAX_BYTES_SIZE = 512_000_000


class OnedriveAccessConfig(AccessConfig):
    client_cred: str = Field(description="Microsoft App client secret")


class OnedriveConnectionConfig(ConnectionConfig):
    client_id: str = Field(description="Microsoft app client ID")
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
    access_config: Secret[OnedriveAccessConfig]

    def get_drive(self) -> "Drive":
        client = self.get_client()
        drive = client.users[self.user_pname].drive
        return drive

    @requires_dependencies(["msal"], extras="onedrive")
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
            logger.error("Couldn't set up credentials for OneDrive")
            raise exc
        if "error" in token:
            raise SourceConnectionNetworkError(
                "failed to fetch token, {}: {}".format(token["error"], token["error_description"])
            )
        return token

    @requires_dependencies(["office365"], extras="onedrive")
    def get_client(self) -> "GraphClient":
        from office365.graph_client import GraphClient

        client = GraphClient(self.get_token)
        return client


class OnedriveIndexerConfig(IndexerConfig):
    path: Optional[str] = Field(default="")
    recursive: bool = False


@dataclass
class OnedriveIndexer(Indexer):
    connection_config: OnedriveConnectionConfig
    index_config: OnedriveIndexerConfig
    connector_type: str = CONNECTOR_TYPE

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
            connector_type=self.connector_type,
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

    async def run_async(self, **kwargs: Any) -> AsyncIterator[FileData]:
        token_resp = await asyncio.to_thread(self.connection_config.get_token)
        if "error" in token_resp:
            raise SourceConnectionError(
                f"[{self.connector_type}]: {token_resp['error']} "
                f"({token_resp.get('error_description')})"
            )

        client = await asyncio.to_thread(self.connection_config.get_client)
        root = await self.get_root(client=client)
        drive_items = await self.list_objects(folder=root, recursive=self.index_config.recursive)

        for drive_item in drive_items:
            file_data = await self.drive_item_to_file_data(drive_item=drive_item)
            yield file_data


class OnedriveDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class OnedriveDownloader(Downloader):
    connection_config: OnedriveConnectionConfig
    download_config: OnedriveDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @SourceConnectionNetworkError.wrap
    def _fetch_file(self, file_data: FileData) -> DriveItem:
        if file_data.source_identifiers is None or not file_data.source_identifiers.fullpath:
            raise ValueError(
                f"file data doesn't have enough information to get "
                f"file content: {file_data.model_dump()}"
            )

        server_relative_path = file_data.source_identifiers.fullpath
        client = self.connection_config.get_client()
        root = client.users[self.connection_config.user_pname].drive.get().execute_query().root
        file = root.get_by_path(server_relative_path).get().execute_query()
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
            logger.error(
                f"[{self.connector_type}] Exception during downloading: {e}", exc_info=True
            )
            # Re-raise to see full stack trace locally
            raise


class OnedriveUploaderConfig(UploaderConfig):
    remote_url: str = Field(
        description="URL of the destination in OneDrive, e.g., 'onedrive://Documents/Folder'"
    )
    prefix: str = "onedrive://"

    @property
    def root_folder(self) -> str:
        url = (
            self.remote_url.replace(self.prefix, "", 1)
            if self.remote_url.startswith(self.prefix)
            else self.remote_url
        )
        return url.split("/")[0]

    @property
    def url(self) -> str:
        url = (
            self.remote_url.replace(self.prefix, "", 1)
            if self.remote_url.startswith(self.prefix)
            else self.remote_url
        )
        return url


@dataclass
class OnedriveUploader(Uploader):
    connection_config: OnedriveConnectionConfig
    upload_config: OnedriveUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["office365"], extras="onedrive")
    def precheck(self) -> None:
        from office365.runtime.client_request_exception import ClientRequestException

        try:
            token_resp: dict = self.connection_config.get_token()
            if error := token_resp.get("error"):
                raise SourceConnectionError(
                    "{} ({})".format(error, token_resp.get("error_description"))
                )
            drive = self.connection_config.get_drive()
            root = drive.root
            root_folder = self.upload_config.root_folder
            folder = root.get_by_path(root_folder)
            try:
                folder.get().execute_query()
            except ClientRequestException as e:
                if not e.response.status_code == 404:
                    raise e
                folder = root.create_folder(root_folder).execute_query()
                logger.info(f"successfully created folder: {folder.name}")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["office365"], extras="onedrive")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        from office365.onedrive.driveitems.conflict_behavior import ConflictBehavior
        from office365.runtime.client_request_exception import ClientRequestException

        drive = self.connection_config.get_drive()

        # Use the remote_url from upload_config as the base destination folder
        base_destination_folder = self.upload_config.url

        # Use the file's relative path to maintain directory structure, if needed
        if file_data.source_identifiers and file_data.source_identifiers.rel_path:
            # Combine the base destination folder with the file's relative path
            destination_path = Path(base_destination_folder) / Path(
                f"{file_data.source_identifiers.rel_path}.json"
            )
        else:
            # If no relative path is provided, upload directly to the base destination folder
            destination_path = Path(base_destination_folder) / f"{path.name}.json"

        destination_folder = destination_path.parent
        file_name = destination_path.name

        # Convert destination folder to a string suitable for OneDrive API
        destination_folder_str = str(destination_folder).replace("\\", "/")

        # Resolve the destination folder in OneDrive, creating it if necessary
        try:
            # Attempt to get the folder
            folder = drive.root.get_by_path(destination_folder_str)
            folder.get().execute_query()
        except ClientRequestException as e:
            # Folder doesn't exist, create it recursively
            root = drive.root
            root_folder = self.upload_config.root_folder
            if not e.response.status_code == 404:
                raise e
            folder = root.create_folder(root_folder).execute_query()
            logger.info(f"successfully created folder: {folder.name}")

        # Check the size of the file
        file_size = path.stat().st_size

        if file_size < MAX_BYTES_SIZE:
            # Use simple upload for small files
            with path.open("rb") as local_file:
                content = local_file.read()
                logger.info(f"Uploading {path} to {destination_path} using simple upload")
                try:
                    uploaded_file = folder.upload(file_name, content).execute_query()
                    if not uploaded_file or uploaded_file.name != file_name:
                        raise DestinationConnectionError(f"Upload failed for file '{file_name}'")
                    # Log details about the uploaded file
                    logger.info(
                        f"Uploaded file '{uploaded_file.name}' with ID '{uploaded_file.id}'"
                    )
                except Exception as e:
                    logger.error(f"Failed to upload file '{file_name}': {e}", exc_info=True)
                    raise DestinationConnectionError(
                        f"Failed to upload file '{file_name}': {e}"
                    ) from e
        else:
            # Use resumable upload for large files
            destination_drive_item = drive.root.get_by_path(destination_folder_str)

            logger.info(
                f"Uploading {path.parent / file_name} to {destination_folder_str} using resumable upload"  # noqa: E501
            )

            try:
                uploaded_file = destination_drive_item.resumable_upload(
                    source_path=str(path)
                ).execute_query()
                # Rename the uploaded file to the original source name with a .json extension
                # Overwrite the file if it already exists
                renamed_file = uploaded_file.move(
                    name=file_name, conflict_behavior=ConflictBehavior.Replace
                ).execute_query()
                # Validate the upload
                if not renamed_file or renamed_file.name != file_name:
                    raise DestinationConnectionError(f"Upload failed for file '{file_name}'")
                # Log details about the uploaded file
                logger.info(f"Uploaded file {renamed_file.name} with ID {renamed_file.id}")
            except Exception as e:
                logger.error(f"Failed to upload file '{file_name}' using resumable upload: {e}")
                raise DestinationConnectionError(
                    f"Failed to upload file '{file_name}' using resumable upload: {e}"
                ) from e


onedrive_source_entry = SourceRegistryEntry(
    connection_config=OnedriveConnectionConfig,
    indexer_config=OnedriveIndexerConfig,
    indexer=OnedriveIndexer,
    downloader_config=OnedriveDownloaderConfig,
    downloader=OnedriveDownloader,
)

onedrive_destination_entry = DestinationRegistryEntry(
    connection_config=OnedriveConnectionConfig,
    uploader=OnedriveUploader,
    uploader_config=OnedriveUploaderConfig,
)
