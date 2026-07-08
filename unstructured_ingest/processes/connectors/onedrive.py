from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, AsyncIterator, Optional

from dateutil import parser
from pydantic import Field, Secret, model_validator

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
    UserAuthError,
    ValueError,
)
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
    LocationShape,
    SourceRegistryEntry,
)
from unstructured_ingest.processes.utils.blob_storage import (
    BlobStoreUploadStager,
    BlobStoreUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from office365.graph_client import GraphClient
    from office365.onedrive.driveitems.driveItem import DriveItem
    from office365.onedrive.drives.drive import Drive

CONNECTOR_TYPE = "onedrive"
MAX_BYTES_SIZE = 512_000_000

# https://graph.microsoft.com/v1.0/$batch hard limit
PERMISSIONS_BATCH_SIZE = 20

# https://learn.microsoft.com/en-us/graph/api/resources/permission
MICROSOFT_ROLE_MAPPING: dict[str, list[str]] = {
    "owner": ["read", "update", "delete"],
    "write": ["read", "update"],
    "read": ["read"],
    "member": ["read", "update"],
    "sp.full control": ["read", "update", "delete"],
    "sp.manage lists": ["read", "update", "delete"],
    "sp.manage hierarchy": ["read", "update", "delete"],
    "sp.approve": ["read", "update"],
    "sp.edit": ["read", "update"],
    "sp.contribute": ["read", "update"],
    "sp.view only": ["read"],
    "sp.restricted read": ["read"],
    "sp.limited access": [],
    "sp.restricted interfaces for translation": [],
}


class _RetriableBatchError(Exception):
    """Marker exception for transient Graph $batch failures (network, 429, 503)."""


class OnedriveAccessConfig(AccessConfig):
    client_cred: Optional[str] = Field(default=None, description="Microsoft App client secret")
    password: Optional[str] = Field(description="Service account password", default=None)
    oauth_token: Optional[str] = Field(
        default=None,
        description=(
            "OAuth 2.0 access token for delegated user authentication. "
            "Tokens typically expire after ~1 hour."
        ),
    )
    refresh_token: Optional[str] = Field(
        default=None,
        description="OAuth 2.0 refresh token for obtaining new access tokens. "
        "Long-lived; used by the platform to refresh expired access tokens "
        "before each job run.",
    )

    def model_post_init(self, __context: Any) -> None:
        # Use truthiness so empty strings (e.g. from unset env vars) are treated
        # consistently with the runtime auth-mode check in get_token below.
        has_client_cred = bool(self.client_cred)
        has_oauth_token = bool(self.oauth_token)
        has_password = bool(self.password)

        if not has_client_cred and not has_oauth_token:
            raise ValueError("either client_cred or oauth_token must be set")

        if has_oauth_token and (has_client_cred or has_password):
            raise ValueError("cannot use both oauth_token and client_cred/password authentication")


class OnedriveConnectionConfig(ConnectionConfig):
    client_id: Optional[str] = Field(
        default=None,
        description=(
            "Microsoft app client ID. Required for app-only and password-grant authentication;"
            " not required when using oauth_token."
        ),
    )
    user_pname: str = Field(
        description="User principal name or service account, usually your Azure AD email."
    )
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

    @model_validator(mode="after")
    def _require_client_id_without_oauth(self) -> "OnedriveConnectionConfig":
        # client_id lives on ConnectionConfig (above) and oauth_token on AccessConfig,
        # so this cross-field rule can't live in either model_post_init alone.
        if not self.access_config.get_secret_value().oauth_token and not self.client_id:
            raise ValueError("client_id is required when oauth_token is not set")
        return self

    def _log_oauth_advisory(self) -> None:
        """Emit a one-shot advisory at precheck time when delegated OAuth is in use.

        Lives on ConnectionConfig so Indexer/Uploader/Downloader prechecks share
        one source of truth instead of each duplicating the message. Called from
        precheck (once per step instance) rather than from get_token (called per
        Graph request) to avoid log spam during normal indexing.
        """
        if self.access_config.get_secret_value().oauth_token:
            logger.warning("Using OAuth token authentication. Tokens expire after ~1 hour.")

    def get_drive(self) -> "Drive":
        client = self.get_client()
        drive = client.users[self.user_pname].drive
        return drive

    @requires_dependencies(["msal", "requests"], extras="onedrive")
    def get_token(self):
        from msal import ConfidentialClientApplication
        from requests import post

        access_config = self.access_config.get_secret_value()

        if access_config.oauth_token:
            # Delegated user authentication: hand the access token through directly.
            # Tokens typically expire after ~1 hour; refresh is not handled here.
            return {"access_token": access_config.oauth_token, "token_type": "Bearer"}

        if access_config.password:
            url = f"https://login.microsoftonline.com/{self.tenant}/oauth2/v2.0/token"
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            data = {
                "grant_type": "password",
                "username": self.user_pname,
                "password": self.access_config.get_secret_value().password,
                "client_id": self.client_id,
                "client_secret": self.access_config.get_secret_value().client_cred,
                "scope": "https://graph.microsoft.com/.default",
            }
            response = post(url, headers=headers, data=data)
            if response.status_code == 200:
                return response.json()
            else:
                raise SourceConnectionError(
                    f"Oauth2 authentication failed with {response.status_code}: {response.text}"
                )

        else:
            try:
                app = ConfidentialClientApplication(
                    authority=f"{self.authority_url}/{self.tenant}",
                    client_id=self.client_id,
                    client_credential=self.access_config.get_secret_value().client_cred,
                )
                token = app.acquire_token_for_client(
                    scopes=["https://graph.microsoft.com/.default"]
                )
            except ValueError as exc:
                logger.error("Couldn't set up credentials.")
                raise exc

            if "error" in token:
                error_codes = token.get("error_codes", [])
                error_type = token.get("error", "")
                error_description = token.get("error_description", "")

                # 7000215: Invalid client secret provided
                # 7000218: Invalid client id provided
                # 700016: Application not found in directory
                # 90002: Tenant not found
                auth_error_codes = [7000215, 7000218, 700016, 90002]

                if any(code in error_codes for code in auth_error_codes) or error_type in [
                    "invalid_client",
                    "unauthorized_client",
                    "invalid_grant",
                ]:
                    raise UserAuthError(f"Authentication failed: {error_type}: {error_description}")
                else:
                    raise SourceConnectionNetworkError(
                        f"Failed to fetch token: {error_type}: {error_description}"
                    )
            return token

    @requires_dependencies(["office365"], extras="onedrive")
    def get_client(self) -> "GraphClient":
        from office365.graph_client import GraphClient

        client = GraphClient(self.get_token)
        return client


class OnedriveIndexerConfig(IndexerConfig):
    path: Optional[str] = Field(default="", json_schema_extra={"x-runtime-eligible": True})
    recursive: bool = Field(default=False, json_schema_extra={"x-runtime-eligible": True})


@dataclass
class OnedriveIndexer(Indexer):
    connection_config: OnedriveConnectionConfig
    index_config: OnedriveIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        self.connection_config._log_oauth_advisory()
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

    def drive_item_to_file_data_sync(
        self,
        drive_item: "DriveItem",
        raw_permissions: Optional[list[dict[str, Any]]] = None,
    ) -> FileData:
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
        file_data = FileData(
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
            display_name=server_path,
        )
        if raw_permissions:
            file_data.metadata.permissions_data = self.extract_permissions(raw_permissions)
        return file_data

    async def drive_item_to_file_data(
        self,
        drive_item: "DriveItem",
        raw_permissions: Optional[list[dict[str, Any]]] = None,
    ) -> FileData:
        # Offload the file data creation if it's not guaranteed async
        return await asyncio.to_thread(
            self.drive_item_to_file_data_sync, drive_item, raw_permissions
        )

    @staticmethod
    def _extract_identity_ids_from_raw(
        raw_props: dict[str, Any],
    ) -> tuple[set[str], set[str]]:
        """Extract Azure AD user/group IDs from a raw Graph permission dict.

        SharePoint siteGroup entries (numeric IDs) are excluded — not resolvable
        via Graph.
        """
        user_ids: set[str] = set()
        group_ids: set[str] = set()

        for key in ("grantedToV2", "grantedTo"):
            identity_set = raw_props.get(key)
            if not identity_set:
                continue

            user = identity_set.get("user")
            if user and user.get("id"):
                user_ids.add(user["id"])

            group = identity_set.get("group")
            if group and group.get("id"):
                group_ids.add(group["id"])

            # v2 supersedes v1 entirely, even when v2 has only unresolvable
            # siteGroup entries and no extractable Azure AD identities.
            break

        for key in ("grantedToIdentitiesV2", "grantedToIdentities"):
            identities = raw_props.get(key)
            if not identities:
                continue
            for identity_set in identities:
                user = identity_set.get("user")
                if user and user.get("id"):
                    user_ids.add(user["id"])
                group = identity_set.get("group")
                if group and group.get("id"):
                    group_ids.add(group["id"])
            break

        return user_ids, group_ids

    def extract_permissions(
        self,
        raw_permissions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Normalize raw Graph permission dicts to the read/update/delete schema
        shared with Google Drive and Confluence connectors."""
        if not raw_permissions:
            logger.debug("no permissions found")
            return [{}]

        normalized: dict[str, dict[str, set[str]]] = {
            "read": {"users": set(), "groups": set()},
            "update": {"users": set(), "groups": set()},
            "delete": {"users": set(), "groups": set()},
        }

        for raw_props in raw_permissions:
            roles = raw_props.get("roles", [])

            operations: set[str] = set()
            for role in roles:
                mapped = MICROSOFT_ROLE_MAPPING.get(role.lower(), [])
                operations.update(mapped)
                if not mapped and role.lower() not in MICROSOFT_ROLE_MAPPING:
                    logger.debug(f"unmapped Microsoft permission role: {role}")

            if not operations:
                continue

            user_ids, group_ids = self._extract_identity_ids_from_raw(raw_props)

            for op in operations:
                normalized[op]["users"].update(user_ids)
                normalized[op]["groups"].update(group_ids)

        result: dict[str, dict[str, list[str]]] = {}
        for op, principals in normalized.items():
            result[op] = {k: sorted(v) for k, v in principals.items()}

        logger.debug(f"normalized permissions generated: {result}")
        return [{k: v} for k, v in result.items()]

    @staticmethod
    def _parse_batch_response(
        payload: dict[str, Any],
        drive_items: list["DriveItem"],
    ) -> dict[str, list[dict[str, Any]]]:
        """Map Graph $batch sub-responses to {drive_item.id: raw_permission_dicts}."""
        by_id: dict[str, list[dict[str, Any]]] = {di.id: [] for di in drive_items}
        for sub in payload.get("responses", []):
            sub_id = sub.get("id")
            # Builtin ValueError is shadowed in this module; use explicit
            # isdigit() check instead of try/except int(...).
            if not (isinstance(sub_id, str) and sub_id.isdigit()):
                continue
            idx = int(sub_id)
            if idx >= len(drive_items):
                continue
            di = drive_items[idx]
            status = sub.get("status")
            if status == 200:
                by_id[di.id] = sub.get("body", {}).get("value", [])
            elif status in (401, 403):
                logger.error(
                    f"forbidden fetching permissions for {di.name} (status {status})"
                )
            elif status == 404:
                logger.warning(f"permissions not found for {di.name}")
            else:
                logger.warning(
                    f"unexpected status {status} fetching permissions for {di.name}: "
                    f"{sub.get('body')}"
                )
        return by_id

    @requires_dependencies(["requests", "tenacity"], extras="onedrive")
    def _fetch_permissions_raw(
        self,
        drive_items: list["DriveItem"],
        access_token: str,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch raw permission JSON for a batch of drive items via Graph /$batch.

        Bypasses the office365 SDK because its IdentitySet/SharePointIdentitySet
        have a mutable-default-arg singleton that collapses all Permission
        identities to whichever user was deserialized last in the process.

        Returns {drive_item.id: [raw_permission_dict, ...]}. Failed sub-requests
        and exhausted retries (network / 429 / 503) degrade to empty lists.
        """
        import requests
        from tenacity import (
            retry,
            retry_if_exception_type,
            stop_after_attempt,
            wait_exponential,
        )

        if not drive_items:
            return {}

        body = {
            "requests": [
                {
                    "id": str(idx),
                    "method": "GET",
                    "url": (
                        f"/drives/{di.parent_reference.driveId}"
                        f"/items/{di.id}/permissions"
                    ),
                }
                for idx, di in enumerate(drive_items)
            ]
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(exp_base=2, multiplier=1, min=2, max=30),
            retry=retry_if_exception_type(_RetriableBatchError),
            reraise=True,
        )
        def _post():
            try:
                r = requests.post(
                    "https://graph.microsoft.com/v1.0/$batch",
                    headers=headers,
                    json=body,
                    timeout=60,
                )
            except requests.exceptions.RequestException as exc:
                raise _RetriableBatchError(f"network error: {exc}") from exc
            if r.status_code in (429, 503):
                raise _RetriableBatchError(f"throttled with status {r.status_code}")
            return r

        try:
            resp = _post()
        except _RetriableBatchError as exc:
            logger.warning(
                f"giving up after retries on Graph $batch: {exc}; "
                f"skipping permissions for {len(drive_items)} items"
            )
            return {di.id: [] for di in drive_items}

        if resp.status_code == 401:
            raise UserAuthError(
                "Unauthorized fetching permissions. Check credentials and required "
                "Graph scope (Files.Read.All / Sites.Read.All)"
            )
        if resp.status_code >= 400:
            logger.warning(
                f"Graph $batch returned {resp.status_code}; "
                f"skipping permissions for {len(drive_items)} items: {resp.text[:200]}"
            )
            return {di.id: [] for di in drive_items}

        return self._parse_batch_response(resp.json(), drive_items)

    def is_async(self) -> bool:
        return True

    async def run_async(self, **kwargs: Any) -> AsyncIterator[FileData]:
        token_resp = await asyncio.to_thread(self.connection_config.get_token)
        if "error" in token_resp:
            raise SourceConnectionError(
                f"[{self.connector_type}]: {token_resp['error']} "
                f"({token_resp.get('error_description')})"
            )

        access_token = token_resp["access_token"]
        client = await asyncio.to_thread(self.connection_config.get_client)
        root = await self.get_root(client=client)
        drive_items = await self.list_objects(folder=root, recursive=self.index_config.recursive)

        for i in range(0, len(drive_items), PERMISSIONS_BATCH_SIZE):
            chunk = drive_items[i : i + PERMISSIONS_BATCH_SIZE]
            perms_by_id = await asyncio.to_thread(
                self._fetch_permissions_raw, chunk, access_token
            )
            for drive_item in chunk:
                yield await self.drive_item_to_file_data(
                    drive_item=drive_item,
                    raw_permissions=perms_by_id.get(drive_item.id, []),
                )


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
        description="URL of the destination in OneDrive, e.g., 'onedrive://Documents/Folder'",
        json_schema_extra={"x-runtime-eligible": True},
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

        self.connection_config._log_oauth_advisory()
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
        if file_data.source_identifiers and file_data.source_identifiers.relative_path:
            # Combine the base destination folder with the file's relative path
            destination_path = Path(base_destination_folder) / Path(
                f"{file_data.source_identifiers.relative_path}.json"
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
    location_shape=LocationShape.API_FOLDER,
    location_identity=("indexer_config.path",),
    emits_record_version=True,
    supports_recursion=True,
)

onedrive_destination_entry = DestinationRegistryEntry(
    connection_config=OnedriveConnectionConfig,
    uploader=OnedriveUploader,
    uploader_config=OnedriveUploaderConfig,
    upload_stager_config=BlobStoreUploadStagerConfig,
    upload_stager=BlobStoreUploadStager,
    location_shape=LocationShape.FSSPEC_URL,
    location_identity=("uploader_config.remote_url",),
    supports_recursion=True,
)
