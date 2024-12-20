from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, AsyncIterator, Generator, Iterator, Optional, TypeVar

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
MAX_MB_SIZE = 512_000_000


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


T = TypeVar("T")


def async_iterable_to_sync_iterable(iterator: AsyncIterator[T]) -> Iterator[T]:
    # This version works on Python 3.9 by manually handling the async iteration.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        while True:
            try:
                # Instead of anext(iterator), we directly call __anext__().
                # __anext__ returns a coroutine that we must run until complete.
                future = iterator.__anext__()
                result = loop.run_until_complete(future)
                yield result
            except StopAsyncIteration:
                break
    finally:
        loop.close()


@dataclass
class OnedriveIndexer(Indexer):
    connection_config: OnedriveConnectionConfig
    index_config: OnedriveIndexerConfig

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

    async def _run_async(self, **kwargs: Any) -> AsyncIterator[FileData]:
        token_resp = await asyncio.to_thread(self.connection_config.get_token)
        if "error" in token_resp:
            raise SourceConnectionError(
                f"[{CONNECTOR_TYPE}]: {token_resp['error']} ({token_resp.get('error_description')})"
            )

        client = await asyncio.to_thread(self.connection_config.get_client)
        root = await self.get_root(client=client)
        drive_items = await self.list_objects(folder=root, recursive=self.index_config.recursive)

        for drive_item in drive_items:
            file_data = await self.drive_item_to_file_data(drive_item=drive_item)
            yield file_data

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        # Convert the async generator to a sync generator without loading all data into memory
        async_gen = self._run_async(**kwargs)
        for item in async_iterable_to_sync_iterable(async_gen):
            yield item


class OnedriveDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class OnedriveDownloader(Downloader):
    connection_config: OnedriveConnectionConfig
    download_config: OnedriveDownloaderConfig

    @SourceConnectionNetworkError.wrap
    def _fetch_file(self, file_data: FileData):
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
            if fsize > MAX_MB_SIZE:
                logger.info(f"downloading file with size: {fsize} bytes in chunks")
                with download_path.open(mode="wb") as f:
                    file.download_session(f, chunk_size=1024 * 1024 * 100).execute_query()
            else:
                with download_path.open(mode="wb") as f:
                    file.download(f).execute_query()
            return self.generate_download_response(file_data=file_data, download_path=download_path)
        except Exception as e:
            logger.error(f"[{CONNECTOR_TYPE}] Exception during downloading: {e}", exc_info=True)
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
                if e.message != "The resource could not be found.":
                    raise e
                folder = root.create_folder(root_folder).execute_query()
                logger.info(f"successfully created folder: {folder.name}")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        drive = self.connection_config.get_drive()

        # Use the remote_url from upload_config as the base destination folder
        base_destination_folder = self.upload_config.url

        # Use the file's relative path to maintain directory structure, if needed
        if file_data.source_identifiers and file_data.source_identifiers.rel_path:
            # Combine the base destination folder with the file's relative path
            destination_path = Path(base_destination_folder) / Path(
                file_data.source_identifiers.rel_path
            )
        else:
            # If no relative path is provided, upload directly to the base destination folder
            destination_path = Path(base_destination_folder) / path.name

        destination_folder = destination_path.parent
        file_name = destination_path.name

        # Convert destination folder to a string suitable for OneDrive API
        destination_folder_str = str(destination_folder).replace("\\", "/")

        # Resolve the destination folder in OneDrive, creating it if necessary
        try:
            # Attempt to get the folder
            folder = drive.root.get_by_path(destination_folder_str)
            folder.get().execute_query()
        except Exception:
            # Folder doesn't exist, create it recursively
            current_folder = drive.root
            for part in destination_folder.parts:
                # Use filter to find the folder by name
                folders = (
                    current_folder.children.filter(f"name eq '{part}' and folder ne null")
                    .get()
                    .execute_query()
                )
                if folders:
                    current_folder = folders[0]
                else:
                    # Folder doesn't exist, create it
                    current_folder = current_folder.create_folder(part).execute_query()
            folder = current_folder

        # Check the size of the file
        file_size = path.stat().st_size

        if file_size < MAX_MB_SIZE:
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
            destination_fullpath = f"{destination_folder_str}/{file_name}"
            destination_drive_item = drive.root.item_with_path(destination_fullpath)

            logger.info(f"Uploading {path} to {destination_fullpath} using resumable upload")
            try:
                uploaded_file = destination_drive_item.resumable_upload(
                    source_path=str(path)
                ).execute_query()
                # Validate the upload
                if not uploaded_file or uploaded_file.name != file_name:
                    raise DestinationConnectionError(f"Upload failed for file '{file_name}'")
                # Log details about the uploaded file
                logger.info(f"Uploaded file {uploaded_file.name} with ID {uploaded_file.id}")
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
