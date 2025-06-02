import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Generator, Optional

from dateutil import parser
from pydantic import Field, Secret
from pydantic.functional_validators import BeforeValidator

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import (
    SourceConnectionError,
)
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import SourceRegistryEntry
from unstructured_ingest.processes.connectors.utils import conform_string_to_dict
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from googleapiclient.discovery import Resource as GoogleAPIResource

CONNECTOR_TYPE = "google_drive"


# Maps Google-native Drive MIME types → export MIME types
GOOGLE_EXPORT_MIME_MAP = {
    "application/vnd.google-apps.document": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",  # noqa: E501
    "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # noqa: E501
    "application/vnd.google-apps.presentation": "application/vnd.openxmlformats-officedocument.presentationml.presentation",  # noqa: E501
}

# Maps export MIME types → file extensions
EXPORT_EXTENSION_MAP = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/pdf": ".pdf",
    "text/html": ".html",
}

# LRO Export Size Threshold is 10MB in real but the exported file might be slightly larger
# than the original Google Workspace file - thus the threshold is set to 9MB
LRO_EXPORT_SIZE_THRESHOLD = 9 * 1024 * 1024  # 9MB


class GoogleDriveAccessConfig(AccessConfig):
    service_account_key: Optional[Annotated[dict, BeforeValidator(conform_string_to_dict)]] = Field(
        default=None, description="Credentials values to use for authentication"
    )
    service_account_key_path: Optional[Path] = Field(
        default=None,
        description="File path to credentials values to use for authentication",
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

    def model_post_init(self, __context: Any) -> None:
        if self.extensions is not None:
            self.extensions = [e.lstrip(".") for e in self.extensions]


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
            "size",
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
            client.list(
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                spaces="drive",
                pageSize=1,
                fields="files(id)",
            ).execute()
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
    def count_files_recursively(
        files_client: "GoogleAPIResource", folder_id: str, extensions: list[str] = None
    ) -> int:
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
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
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
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
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
                "Failed to validate Google Drive connection during precheck",
                exc_info=True,
            )
            raise SourceConnectionError(f"Precheck failed: {e}")

    @staticmethod
    def is_dir(record: dict) -> bool:
        return record.get("mimeType") == "application/vnd.google-apps.folder"

    @staticmethod
    def map_file_data(root_info: dict) -> FileData:
        file_id = root_info["id"]
        filename = root_info.pop("name")
        url = root_info.pop("webContentLink", None)
        version = root_info.pop("version", None)
        permissions = root_info.pop("permissions", None)
        date_created_str = root_info.pop("createdTime", None)
        date_created_dt = parser.parse(date_created_str) if date_created_str else None
        date_modified_str = root_info.pop("modifiedTime", None)
        parent_path = root_info.pop("parent_path", None)
        parent_root_path = root_info.pop("parent_root_path", None)
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
            additional_metadata=root_info,
            display_name=source_identifiers.fullpath,
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
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
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
        return files_client.get(
            supportsAllDrives=True, fileId=object_id, fields=",".join(self.fields)
        ).execute()

    def get_files(
        self,
        files_client,
        object_id: str,
        recursive: bool = False,
        extensions: Optional[list[str]] = None,
    ) -> list[FileData]:
        root_info = self.get_root_info(files_client=files_client, object_id=object_id)
        if not self.is_dir(root_info):
            root_info["permissions"] = self.extract_permissions(root_info.get("permissions"))
            data = [self.map_file_data(root_info)]
        else:
            file_contents = self.get_paginated_results(
                files_client=files_client,
                object_id=object_id,
                extensions=extensions,
                recursive=recursive,
                previous_path=root_info["name"],
            )
            data = []
            for f in file_contents:
                f["permissions"] = self.extract_permissions(f.get("permissions"))
                data.append(self.map_file_data(root_info=f))
        for d in data:
            d.metadata.record_locator["drive_id"]: object_id
        return data

    def extract_permissions(self, permissions: Optional[list[dict]]) -> list[dict]:
        if not permissions:
            logger.debug("no permissions found")
            return [{}]

        # https://developers.google.com/workspace/drive/api/guides/ref-roles
        role_mapping = {
            "owner": ["read", "update", "delete"],
            "organizer": ["read", "update", "delete"],
            "fileOrganizer": ["read", "update"],
            "writer": ["read", "update"],
            "commenter": ["read"],
            "reader": ["read"],
        }

        normalized_permissions = {
            "read": {"users": set(), "groups": set()},
            "update": {"users": set(), "groups": set()},
            "delete": {"users": set(), "groups": set()},
        }

        for item in permissions:
            # https://developers.google.com/workspace/drive/api/reference/rest/v3/permissions
            # ignore permissions for "anyone" and "domain"
            if item["type"] in ["user", "group"]:
                type_key = item["type"] + "s"
                for operation in role_mapping[item["role"]]:
                    normalized_permissions[operation][type_key].add(item["id"])

        # turn sets into sorted lists for consistency and json serialization
        for role_dict in normalized_permissions.values():
            for key in role_dict:
                role_dict[key] = sorted(role_dict[key])

        logger.debug(f"normalized permissions generated: {normalized_permissions}")
        return [{k: v} for k, v in normalized_permissions.items()]

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
    lro_max_tries: int = 10
    lro_max_time: int = 10 * 60  # 10 minutes


def _get_extension(file_data: FileData) -> str:
    """
    Returns the extension for a given source MIME type.
    """
    source_mime_type = file_data.additional_metadata.get("export_mime_type", "")
    export_mime_type = GOOGLE_EXPORT_MIME_MAP.get(source_mime_type, "")
    if export_mime_type:
        return EXPORT_EXTENSION_MAP.get(export_mime_type, "")
    return ""


@dataclass
class GoogleDriveDownloader(Downloader):
    """
    Downloads files from Google Drive using googleapis client. For native files, it uses the export
    functionality for files <10MB and LRO (Long Running Operation) for files >10MB.
    """

    connection_config: GoogleDriveConnectionConfig
    download_config: GoogleDriveDownloaderConfig = field(
        default_factory=lambda: GoogleDriveDownloaderConfig()
    )
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["googleapiclient"], extras="google-drive")
    def _direct_download_file(self, file_id, download_path: Path):
        """Downloads a file from Google Drive using the Drive API's media download functionality.
        The method uses Google Drive API's media download functionality to stream the file
        content directly to disk.

        Args:
            file_id (str): The ID of the file to download from Google Drive.
            download_path (Path): The local path where the file should be saved.

        Raises:
            SourceConnectionError: If the download operation fails.
        """
        from googleapiclient.errors import HttpError
        from googleapiclient.http import MediaIoBaseDownload

        try:
            with self.connection_config.get_client() as client:
                # pylint: disable=maybe-no-member
                request = client.get_media(fileId=file_id)

                with open(download_path, "wb") as file:
                    downloader = MediaIoBaseDownload(file, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
                        logger.debug(f"Download progress:{int(status.progress() * 100)}.")

        except (HttpError, ValueError) as error:
            logger.exception(f"Error downloading file {file_id} to {download_path}: {error}")
            raise SourceConnectionError("Failed to download file") from error

    @requires_dependencies(["googleapiclient"], extras="google-drive")
    def _export_gdrive_file_with_lro(self, file_id: str, download_path: Path, mime_type: str):
        """Exports a Google Drive file using Long-Running Operation (LRO) for large files
        (>10MB of the exported file size).

        This method is used when the standard export method fails due to file size limitations.
        It uses the Drive API's LRO functionality to handle large file exports.

        Args:
            file_id (str): The ID of the Google Drive file to export.
            download_path (Path): The local path where the exported file should be saved.
            mime_type (str): The target MIME type for the exported file.
        Raises:
            SourceConnectionError: If the export operation fails.
        """

        import tenacity
        from googleapiclient.errors import HttpError

        max_time = self.download_config.lro_max_time
        max_tries = self.download_config.lro_max_tries

        class OperationNotFinished(Exception):
            """
            Exception raised when the operation is not finished.
            """

            pass

        def is_fatal_code(e: Exception) -> bool:
            """
            Returns True if the error is fatal and should not be retried.
            403 and 429 can mean "Too many requests" or "User rate limit exceeded"
            which should be retried.
            """
            return (
                isinstance(e, HttpError)
                and 400 <= e.resp.status < 500
                and e.resp.status not in [403, 429]
            )

        @tenacity.retry(
            wait=tenacity.wait_exponential(),
            retry=tenacity.retry_if_exception(
                lambda e: (
                    isinstance(e, (HttpError, OperationNotFinished)) and not is_fatal_code(e)
                )
            ),
            stop=(tenacity.stop_after_attempt(max_tries) | tenacity.stop_after_delay(max_time)),
        )
        def _poll_operation(operation: dict, operations_client: "GoogleAPIResource") -> dict:
            """
            Helper function to poll the operation until it's complete.
            Uses backoff exponential retry logic.

            Each `operations.get` call uses the Google API requests limit. Details:
            https://developers.google.com/workspace/drive/api/guides/limits

            The limits as of May 2025 are:
            - 12.000 calls per 60 seconds

            In case of request limitting, the API will return 403 `User rate limit exceeded` error
            or 429 `Too many requests` error.
            """
            if operation.get("done", False):
                return operation
            if "error" in operation:
                raise SourceConnectionError(
                    f"Export operation failed: {operation['error']['message']}"
                )
            # Refresh the operation status:
            # FYI: In some cases the `operations.get` call errors with 403 "User does not have
            # permission" error even if the same user create the operation with `download` method.
            updated_operation = operations_client.get(name=operation["name"]).execute()
            if not updated_operation.get("done", False):
                raise OperationNotFinished()
            return updated_operation

        try:
            with self._get_files_and_operations_client() as (files_client, operations_client):
                # Start the LRO
                operation = files_client.download(fileId=file_id, mimeType=mime_type).execute()

                # In case the operation is not finished, poll it until it's complete
                updated_operation = _poll_operation(operation, operations_client)

                # Get the download URI from the completed operation
                download_uri = updated_operation["response"]["downloadUri"]

            # Download the file using the URI
            self._raw_download_google_drive_file(download_uri, download_path)

        except HttpError as error:
            raise SourceConnectionError(
                f"Failed to export file using Google Drive LRO: {error}"
            ) from error

    @requires_dependencies(["googleapiclient"], extras="google-drive")
    def _export_gdrive_native_file(
        self, file_id: str, download_path: Path, mime_type: str, file_size: int
    ):
        """Exports a Google Drive native file (Docs, Sheets, Slides) to a specified format.

        This method uses the Google Drive API's export functionality to convert Google Workspace
        files to other formats (e.g., Google Docs to PDF, Google Sheets to Excel).
        For files larger than 10MB, it falls back to using Long-Running Operation (LRO).

        Args:
            file_id (str): The ID of the Google Drive file to export.
            download_path (Path): The local path where the exported file should be saved.
            mime_type (str): The target MIME type for the exported file (e.g., 'application/pdf').
            file_size (int): The size of the file to export - used to determine if the
                file is large enough to use LRO instead of direct export endpoint.
        Returns:
            bytes: The exported file content.

        Raises:
            HttpError: If the export operation fails.
        """
        from googleapiclient.errors import HttpError
        from googleapiclient.http import MediaIoBaseDownload

        if file_size > LRO_EXPORT_SIZE_THRESHOLD:
            self._export_gdrive_file_with_lro(file_id, download_path, mime_type)
            return

        with self.connection_config.get_client() as client:
            try:
                # pylint: disable=maybe-no-member
                request = client.export_media(fileId=file_id, mimeType=mime_type)
                with open(download_path, "wb") as file:
                    downloader = MediaIoBaseDownload(file, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
                        logger.debug(f"Download progress: {int(status.progress() * 100)}.")
            except HttpError as error:
                if error.resp.status == 403 and "too large" in error.reason.lower():
                    # Even though we have the LRO threashold, for some smaller files the
                    # export size might exceed 10MB and we get a 403 error.
                    # In that case, we use LRO as a fallback.
                    self._export_gdrive_file_with_lro(file_id, download_path, mime_type)
                else:
                    raise SourceConnectionError(f"Failed to export file: {error}") from error

    @requires_dependencies(["googleapiclient"], extras="google-drive")
    @contextmanager
    def _get_files_and_operations_client(
        self,
    ) -> Generator[tuple["GoogleAPIResource", "GoogleAPIResource"], None, None]:
        """
        Returns a context manager for the files and operations clients for the Google Drive API.

        Yields:
            Tuple[GoogleAPIResource, GoogleAPIResource]: A tuple of the files
                and operations clients.
        """
        from googleapiclient.discovery import build

        creds = self._get_credentials()
        service = build("drive", "v3", credentials=creds)
        with (
            service.operations() as operations_client,
            service.files() as files_client,
        ):
            yield files_client, operations_client

    @requires_dependencies(["httpx"])
    def _raw_download_google_drive_file(self, url: str, download_path: Path) -> Path:
        """
        Streams file content directly to disk using authenticated HTTP request.
        Must use httpx to stream the file to disk as currently there's no google SDK
        functionality to download a file like for get media or export operations.

        Writes the file to the correct path in the download directory while downloading.
        Avoids buffering large files in memory.

        Args:
            url (str): The URL of the file to download.
            download_path (Path): The path to save the downloaded file.

        Returns:
            Path: The path to the downloaded file.
        """
        import httpx
        from google.auth.transport.requests import Request

        creds = self._get_credentials()

        creds.refresh(Request())

        headers = {
            "Authorization": f"Bearer {creds.token}",
        }

        with (
            httpx.Client(timeout=None, follow_redirects=True) as client,
            client.stream("GET", url, headers=headers) as response,
        ):
            if response.status_code != 200:
                raise SourceConnectionError(
                    f"Failed to stream download from {url}: {response.status_code}"
                )
            with open(download_path, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)
        return download_path

    @requires_dependencies(["google"], extras="google-drive")
    def _get_credentials(self):
        """
        Retrieves the credentials for Google Drive API access.

        Returns:
            Credentials: The credentials for Google Drive API access.
        """
        from google.oauth2 import service_account

        access_config = self.connection_config.access_config.get_secret_value()
        key_data = access_config.get_service_account_key()
        creds = service_account.Credentials.from_service_account_info(
            key_data,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        return creds

    def _download_file(self, file_data: FileData) -> Path:
        """Downloads a file from Google Drive using either direct download or export based
        on the source file's MIME type.

        This method determines the appropriate download method based on the file's MIME type:
        - For Google Workspace files (Docs, Sheets, Slides), uses export functionality
        - For other files, uses direct download

        Args:
            file_data (FileData): The metadata of the file being downloaded.

        Returns:
            Path: The path to the downloaded file.

        Raises:
            SourceConnectionError: If the download fails.
        """
        mime_type = file_data.additional_metadata.get("mimeType", "")
        file_size = int(file_data.additional_metadata.get("size", 0))
        file_id = file_data.identifier

        download_path = self.get_download_path(file_data)
        if not download_path:
            raise SourceConnectionError(f"Failed to get download path for file {file_id}")

        if mime_type in GOOGLE_EXPORT_MIME_MAP:
            # For Google Workspace files, use export functionality
            ext = _get_extension(file_data)
            download_path = download_path.with_suffix(ext)
            download_path.parent.mkdir(parents=True, exist_ok=True)
            export_mime = GOOGLE_EXPORT_MIME_MAP[mime_type]
            self._export_gdrive_native_file(
                file_id=file_id,
                download_path=download_path,
                mime_type=export_mime,
                file_size=file_size,
            )
            file_data.additional_metadata.update(
                {
                    "export_mime_type": export_mime,
                    "export_extension": ext,
                    "download_method": "google_workspace_export",
                }
            )
        else:
            # For other files, use direct download
            download_path.parent.mkdir(parents=True, exist_ok=True)
            self._direct_download_file(file_id=file_id, download_path=download_path)
            file_data.additional_metadata.update(
                {
                    "download_method": "direct_download",
                }
            )

        return download_path

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        mime_type = file_data.additional_metadata.get("mimeType", "")

        logger.debug(
            f"Downloading file {file_data.source_identifiers.fullpath} of type {mime_type}"
        )

        download_path = self._download_file(file_data)

        file_data.local_download_path = str(download_path.resolve())

        return self.generate_download_response(file_data=file_data, download_path=download_path)


google_drive_source_entry = SourceRegistryEntry(
    connection_config=GoogleDriveConnectionConfig,
    indexer_config=GoogleDriveIndexerConfig,
    indexer=GoogleDriveIndexer,
    downloader_config=GoogleDriveDownloaderConfig,
    downloader=GoogleDriveDownloader,
)
