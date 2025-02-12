import io
import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Generator, Optional

from dateutil import parser
from pydantic import Field, Secret
from pydantic.functional_validators import BeforeValidator

from unstructured_ingest.error import (
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.google_filetype import GOOGLE_DRIVE_EXPORT_TYPES
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
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry
from unstructured_ingest.v2.processes.connectors.utils import conform_string_to_dict

CONNECTOR_TYPE = "google_drive"

if TYPE_CHECKING:
    from googleapiclient.discovery import Resource as GoogleAPIResource
    from googleapiclient.http import MediaIoBaseDownload


class GoogleDriveAccessConfig(AccessConfig):
    service_account_key: Optional[Annotated[dict, BeforeValidator(conform_string_to_dict)]] = Field(
        default=None, description="Credentials values to use for authentication"
    )
    service_account_key_path: Optional[Path] = Field(
        default=None, description="File path to credentials values to use for authentication"
    )

    def model_post_init(self, __context: Any) -> None:
        if self.service_account_key is None and self.service_account_key_path is None:
            raise ValueError(
                "either service_account_key or service_account_key_path must be provided"
            )

    def get_service_account_key(self) -> dict:
        key_data = None
        if self.service_account_key_path:
            with self.service_account_key_path.open() as f:
                key_data = json.load(f)
        if key_data and self.service_account_key:
            if key_data == self.service_account_key:
                return key_data
            else:
                raise ValueError(
                    "service_account_key and service_account_key_path "
                    "both provided and have different values"
                )
        if key_data:
            return key_data
        return self.service_account_key


class GoogleDriveConnectionConfig(ConnectionConfig):
    drive_id: str = Field(description="Google Drive File or Folder ID.")
    access_config: Secret[GoogleDriveAccessConfig]

    @requires_dependencies(["googleapiclient"], extras="google-drive")
    @contextmanager
    def get_client(self) -> Generator["GoogleAPIResource", None, None]:
        from google.auth import exceptions
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError

        access_config = self.access_config.get_secret_value()
        key_data = access_config.get_service_account_key()

        try:
            creds = service_account.Credentials.from_service_account_info(key_data)
            service = build("drive", "v3", credentials=creds)
            with service.files() as client:
                yield client
        except HttpError as exc:
            raise ValueError(f"{exc.reason}")
        except exceptions.DefaultCredentialsError:
            raise ValueError("The provided API key is invalid.")


class GoogleDriveIndexerConfig(IndexerConfig):
    extensions: Optional[list[str]] = None
    recursive: bool = False

    def __post_init__(self):
        # Strip leading period of extension
        if self.extensions is not None:
            self.extensions = [e[1:] if e.startswith(".") else e for e in self.extensions]


@dataclass
class GoogleDriveIndexer(Indexer):
    connection_config: GoogleDriveConnectionConfig
    index_config: GoogleDriveIndexerConfig
    fields: list[str] = field(
        default_factory=lambda: [
            "id",
            "name",
            "mimeType",
            "fileExtension",
            "md5Checksum",
            "sha1Checksum",
            "sha256Checksum",
            "headRevisionId",
            "permissions",
            "createdTime",
            "modifiedTime",
            "version",
            "originalFilename",
            "capabilities",
            "permissionIds",
            "webViewLink",
            "webContentLink",
        ]
    )

    @staticmethod
    def verify_drive_api_enabled(client) -> None:
        from googleapiclient.errors import HttpError

        """
        Makes a lightweight API call to verify that the Drive API is enabled.
        If the API is not enabled, an HttpError should be raised.
        """
        try:
            # A very minimal call: list 1 file from the drive.
            client.list(spaces="drive", pageSize=1, fields="files(id)").execute()
        except HttpError as e:
            error_content = e.content.decode() if hasattr(e, "content") else ""
            lower_error = error_content.lower()
            if "drive api" in lower_error and (
                "not enabled" in lower_error or "not been used" in lower_error
            ):
                raise SourceConnectionError(
                    "Google Drive API is not enabled for your project. \
                    Please enable it in the Google Cloud Console."
                )
            else:
                raise SourceConnectionError("Google drive API unreachable for an unknown reason!")

    @staticmethod
    def count_files_recursively(files_client, folder_id: str, extensions: list[str] = None) -> int:
        """
        Count non-folder files recursively under the given folder.
        If `extensions` is provided, only count files
        whose `fileExtension` matches one of the values.
        """
        count = 0
        stack = [folder_id]
        while stack:
            current_folder = stack.pop()
            # Always list all items under the current folder.
            query = f"'{current_folder}' in parents"
            page_token = None
            while True:
                response = files_client.list(
                    spaces="drive",
                    q=query,
                    fields="nextPageToken, files(id, mimeType, fileExtension)",
                    pageToken=page_token,
                    pageSize=1000,
                ).execute()
                for item in response.get("files", []):
                    if item.get("mimeType") == "application/vnd.google-apps.folder":
                        # Always traverse sub-folders regardless of extension filter.
                        stack.append(item["id"])
                    else:
                        if extensions:
                            # Use a case-insensitive comparison for the file extension.
                            file_ext = (item.get("fileExtension") or "").lower()
                            valid_exts = [e.lower() for e in extensions]
                            if file_ext in valid_exts:
                                count += 1
                        else:
                            count += 1
                page_token = response.get("nextPageToken")
                if not page_token:
                    break
        return count

    def precheck(self) -> None:
        """
        Enhanced precheck that verifies not only connectivity
        but also that the provided drive_id is valid and accessible.
        """
        try:
            with self.connection_config.get_client() as client:
                # First, verify that the Drive API is enabled.
                self.verify_drive_api_enabled(client)

                # Try to retrieve metadata for the drive id.
                # This will catch errors such as an invalid drive id or insufficient permissions.
                root_info = self.get_root_info(
                    files_client=client, object_id=self.connection_config.drive_id
                )
                logger.info(
                    f"Successfully retrieved drive root info: "
                    f"{root_info.get('name', 'Unnamed')} (ID: {root_info.get('id')})"
                )

            # If the target is a folder, perform file count check.
            if self.is_dir(root_info):
                if self.index_config.recursive:
                    file_count = self.count_files_recursively(
                        client,
                        self.connection_config.drive_id,
                        extensions=self.index_config.extensions,
                    )
                    if file_count == 0:
                        logger.warning(
                            "Empty folder: no files found recursively in the folder. \
                             Please verify that the folder contains files and \
                             that the service account has proper permissions."
                        )
                        # raise SourceConnectionError(
                        #     "Empty folder: no files found recursively in the folder. "
                        #     "Please verify that the folder contains files and \
                        #     that the service account has proper permissions."
                        # )
                    else:
                        logger.info(f"Found {file_count} files recursively in the folder.")
                else:
                    # Non-recursive: check for at least one immediate non-folder child.
                    response = client.list(
                        spaces="drive",
                        fields="files(id)",
                        pageSize=1,
                        q=f"'{self.connection_config.drive_id}' in parents",
                    ).execute()
                    if not response.get("files"):
                        logger.warning(
                            "Empty folder: no files found at the folder's root level. "
                            "Please verify that the folder contains files and \
                            that the service account has proper permissions."
                        )
                        # raise SourceConnectionError(
                        #     "Empty folder: no files found at the folder's root level. "
                        #     "Please verify that the folder contains files and \
                        #     that the service account has proper permissions."
                        # )
                    else:
                        logger.info("Found files at the folder's root level.")
            else:
                # If the target is a file, precheck passes.
                logger.info("Drive ID corresponds to a file. Precheck passed.")

        except Exception as e:
            logger.error(
                "Failed to validate Google Drive connection during precheck", exc_info=True
            )
            raise SourceConnectionError(f"Precheck failed: {e}")

    @staticmethod
    def is_dir(record: dict) -> bool:
        return record.get("mimeType") == "application/vnd.google-apps.folder"

    @staticmethod
    def map_file_data(f: dict) -> FileData:
        file_id = f["id"]
        filename = f.pop("name")
        url = f.pop("webContentLink", None)
        version = f.pop("version", None)
        permissions = f.pop("permissions", None)
        date_created_str = f.pop("createdTime", None)
        date_created_dt = parser.parse(date_created_str) if date_created_str else None
        date_modified_str = f.pop("modifiedTime", None)
        parent_path = f.pop("parent_path", None)
        parent_root_path = f.pop("parent_root_path", None)
        date_modified_dt = parser.parse(date_modified_str) if date_modified_str else None
        if (
            parent_path
            and isinstance(parent_path, str)
            and parent_root_path
            and isinstance(parent_root_path, str)
        ):
            fullpath = f"{parent_path}/{filename}"
            rel_path = Path(fullpath).relative_to(parent_root_path).as_posix()
            source_identifiers = SourceIdentifiers(
                filename=filename, fullpath=fullpath, rel_path=rel_path
            )
        else:
            source_identifiers = SourceIdentifiers(fullpath=filename, filename=filename)
        return FileData(
            connector_type=CONNECTOR_TYPE,
            identifier=file_id,
            source_identifiers=source_identifiers,
            metadata=FileDataSourceMetadata(
                url=url,
                version=version,
                date_created=str(date_created_dt.timestamp()),
                date_modified=str(date_modified_dt.timestamp()),
                permissions_data=permissions,
                record_locator={
                    "file_id": file_id,
                },
            ),
            additional_metadata=f,
        )

    def get_paginated_results(
        self,
        files_client,
        object_id: str,
        extensions: Optional[list[str]] = None,
        recursive: bool = False,
        previous_path: Optional[str] = None,
    ) -> list[dict]:

        fields_input = "nextPageToken, files({})".format(",".join(self.fields))
        q = f"'{object_id}' in parents"
        # Filter by extension but still include any directories
        if extensions:
            ext_filter = " or ".join([f"fileExtension = '{e}'" for e in extensions])
            q = f"{q} and ({ext_filter} or mimeType = 'application/vnd.google-apps.folder')"
        logger.debug(f"query used when indexing: {q}")
        logger.debug("response fields limited to: {}".format(", ".join(self.fields)))
        done = False
        page_token = None
        files_response = []
        while not done:
            response: dict = files_client.list(
                spaces="drive",
                fields=fields_input,
                corpora="user",
                pageToken=page_token,
                q=q,
            ).execute()
            if files := response.get("files", []):
                fs = [f for f in files if not self.is_dir(record=f)]
                for r in fs:
                    r["parent_path"] = previous_path
                dirs = [f for f in files if self.is_dir(record=f)]
                files_response.extend(fs)
                if recursive:
                    for d in dirs:
                        dir_id = d["id"]
                        dir_name = d["name"]
                        files_response.extend(
                            self.get_paginated_results(
                                files_client=files_client,
                                object_id=dir_id,
                                extensions=extensions,
                                recursive=recursive,
                                previous_path=f"{previous_path}/{dir_name}",
                            )
                        )
            page_token = response.get("nextPageToken")
            if page_token is None:
                done = True
        for r in files_response:
            r["parent_root_path"] = previous_path
        return files_response

    def get_root_info(self, files_client, object_id: str) -> dict:
        return files_client.get(fileId=object_id, fields=",".join(self.fields)).execute()

    def get_files(
        self,
        files_client,
        object_id: str,
        recursive: bool = False,
        extensions: Optional[list[str]] = None,
    ) -> list[FileData]:
        root_info = self.get_root_info(files_client=files_client, object_id=object_id)
        if not self.is_dir(root_info):
            data = [self.map_file_data(root_info)]
        else:

            file_contents = self.get_paginated_results(
                files_client=files_client,
                object_id=object_id,
                extensions=extensions,
                recursive=recursive,
                previous_path=root_info["name"],
            )
            data = [self.map_file_data(f=f) for f in file_contents]
        for d in data:
            d.metadata.record_locator["drive_id"]: object_id
        return data

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        with self.connection_config.get_client() as client:
            for f in self.get_files(
                files_client=client,
                object_id=self.connection_config.drive_id,
                recursive=self.index_config.recursive,
                extensions=self.index_config.extensions,
            ):
                yield f


class GoogleDriveDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class GoogleDriveDownloader(Downloader):
    connection_config: GoogleDriveConnectionConfig
    download_config: GoogleDriveDownloaderConfig = field(
        default_factory=lambda: GoogleDriveDownloaderConfig()
    )
    connector_type: str = CONNECTOR_TYPE

    @SourceConnectionNetworkError.wrap
    def _get_content(self, downloader: "MediaIoBaseDownload") -> bool:
        downloaded = False
        while downloaded is False:
            _, downloaded = downloader.next_chunk()
        return downloaded

    def _write_file(self, file_data: FileData, file_contents: io.BytesIO) -> DownloadResponse:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"writing {file_data.source_identifiers.fullpath} to {download_path}")
        with open(download_path, "wb") as handler:
            handler.write(file_contents.getbuffer())
        return self.generate_download_response(file_data=file_data, download_path=download_path)

    @requires_dependencies(["googleapiclient"], extras="google-drive")
    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        from googleapiclient.http import MediaIoBaseDownload

        logger.debug(f"fetching file: {file_data.source_identifiers.fullpath}")
        record_id = file_data.identifier
        mime_type = file_data.additional_metadata["mimeType"]
        if not mime_type:
            raise TypeError(
                f"File not supported. Name: {file_data.source_identifiers.filename} "
                f"ID: {record_id} "
                f"MimeType: {mime_type}"
            )
        with self.connection_config.get_client() as client:
            if (
                mime_type.startswith("application/vnd.google-apps")
                and mime_type in GOOGLE_DRIVE_EXPORT_TYPES
            ):
                export_mime = GOOGLE_DRIVE_EXPORT_TYPES.get(
                    mime_type,  # type: ignore
                )
                request = client.export_media(
                    fileId=record_id,
                    mimeType=export_mime,
                )
            else:
                request = client.get_media(fileId=record_id)

        file_contents = io.BytesIO()
        downloader = MediaIoBaseDownload(file_contents, request)
        downloaded = self._get_content(downloader=downloader)
        if not downloaded or not file_contents:
            raise SourceConnectionError("nothing found to download")
        return self._write_file(file_data=file_data, file_contents=file_contents)


google_drive_source_entry = SourceRegistryEntry(
    connection_config=GoogleDriveConnectionConfig,
    indexer_config=GoogleDriveIndexerConfig,
    indexer=GoogleDriveIndexer,
    downloader_config=GoogleDriveDownloaderConfig,
    downloader=GoogleDriveDownloader,
)
