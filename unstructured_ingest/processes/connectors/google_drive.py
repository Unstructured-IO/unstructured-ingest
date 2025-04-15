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
    pass


@dataclass
class GoogleDriveDownloader(Downloader):
    """
    Downloads files from Google Drive using authenticated direct HTTP requests
    via `exportLinks` (for Google-native files) and `webContentLink` (for binary files).

    These links emulate the behavior of Google Drive's "File > Download as..." options
    in the UI and bypass the size limitations of `files.export()`.

    Behavior:
    - Google-native formats are downloaded using `exportLinks` in appropriate MIME formats.
    - Binary files (non-Google-native) are downloaded using `webContentLink`.
    - All downloads are performed via `requests.get()` using a valid bearer token.
    """

    connection_config: GoogleDriveConnectionConfig
    download_config: GoogleDriveDownloaderConfig = field(
        default_factory=lambda: GoogleDriveDownloaderConfig()
    )
    connector_type: str = CONNECTOR_TYPE

    def _get_download_url_and_ext(self, file_id: str, mime_type: str) -> tuple[str, str]:
        """
        Resolves the appropriate download URL and expected file extension for a Google Drive file.

        - Google-native files use export MIME types from exportLinks (e.g., .docx, .xlsx).
        - Binary files use webContentLink (e.g., uploaded PDFs or ZIPs).

        Returns:
            Tuple[str, str]: (download URL, file extension or "")

        Raises:
            SourceConnectionError: If no valid export or download link is available.
        """
        with self.connection_config.get_client() as client:
            metadata = client.get(fileId=file_id, fields="exportLinks,webContentLink").execute()

        export_links = metadata.get("exportLinks", {})
        web_link = metadata.get("webContentLink")

        if export_mime := GOOGLE_EXPORT_MIME_MAP.get(mime_type):
            url = export_links.get(export_mime)
            if not url:
                raise SourceConnectionError(f"No export link found for {file_id} as {export_mime}")
            ext = EXPORT_EXTENSION_MAP.get(export_mime, "")
            return url, ext

        if not web_link:
            raise SourceConnectionError(f"No webContentLink available for file {file_id}")
        return web_link, ""

    @requires_dependencies(["httpx", "google.auth"], extras="google-drive")
    def _download_url(self, file_data: FileData, url: str, ext: str = "") -> Path:
        """
        Streams file content directly to disk using authenticated HTTP request.

        Writes the file to the correct path in the download directory while downloading.
        Avoids buffering large files in memory.

        Returns:
            Path to the downloaded file.

        Raises:
            SourceConnectionError: If the HTTP request fails.
        """
        import httpx
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account

        access_config = self.connection_config.access_config.get_secret_value()
        key_data = access_config.get_service_account_key()
        creds = service_account.Credentials.from_service_account_info(
            key_data,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        creds.refresh(Request())

        headers = {
            "Authorization": f"Bearer {creds.token}",
        }

        download_path = self.get_download_path(file_data)
        if ext:
            download_path = download_path.with_suffix(ext)

        download_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Streaming file to {download_path}")

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

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        mime_type = file_data.additional_metadata.get("mimeType", "")
        record_id = file_data.identifier

        logger.debug(
            f"Downloading file {file_data.source_identifiers.fullpath} of type {mime_type}"
        )

        download_url, ext = self._get_download_url_and_ext(record_id, mime_type)
        download_path = self._download_url(file_data, download_url, ext)

        file_data.additional_metadata.update(
            {
                "download_method": "export_link" if ext else "web_content_link",
                "download_url_used": download_url,
            }
        )
        file_data.local_download_path = str(download_path.resolve())

        return self.generate_download_response(file_data=file_data, download_path=download_path)


google_drive_source_entry = SourceRegistryEntry(
    connection_config=GoogleDriveConnectionConfig,
    indexer_config=GoogleDriveIndexerConfig,
    indexer=GoogleDriveIndexer,
    downloader_config=GoogleDriveDownloaderConfig,
    downloader=GoogleDriveDownloader,
)
