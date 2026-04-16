from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncIterator, Optional

from pydantic import Field

from unstructured_ingest.data_types.file_data import (
    FileData,
)
from unstructured_ingest.error import (
    SourceConnectionError,
    SourceConnectionNetworkError,
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
    from office365.onedrive.permissions.permission import Permission

CONNECTOR_TYPE = "sharepoint"
LEGACY_DEFAULT_PATH = "Shared Documents"

# Microsoft Graph API role → normalized operations
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
    include_permissions: bool = Field(
        default=False,
        description="Extract file-level permissions (ACL) from Microsoft Graph API "
        "and include them in element metadata. Requires the app registration to have "
        "Sites.FullControl.All or Sites.Read.All permission.",
    )


@dataclass
class SharepointIndexer(OnedriveIndexer):
    connection_config: SharepointConnectionConfig
    index_config: SharepointIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def _extract_identity_ids(
        self, permission: "Permission"
    ) -> tuple[set[str], set[str]]:
        """Extract user and group Azure AD Object IDs from a Permission object.

        Checks granted_to_v2 (SharePoint-specific, includes siteUser/siteGroup)
        first, then falls back to granted_to. Also checks granted_to_identities*
        for link-based permissions that grant access to multiple principals.
        """
        user_ids: set[str] = set()
        group_ids: set[str] = set()

        # Direct grant — prefer v2 (has SharePoint-specific identity types)
        for identity_set_attr in ("granted_to_v2", "granted_to"):
            identity_set = getattr(permission, identity_set_attr, None)
            if identity_set is None:
                continue

            # User identity (Azure AD user)
            user = getattr(identity_set, "user", None)
            if user and getattr(user, "id", None):
                user_ids.add(user.id)

            # SharePoint site user
            site_user = getattr(identity_set, "siteUser", None)
            if site_user and getattr(site_user, "id", None):
                user_ids.add(site_user.id)

            # Azure AD group
            group = getattr(identity_set, "group", None)
            if group and getattr(group, "id", None):
                group_ids.add(group.id)

            # SharePoint site group
            site_group = getattr(identity_set, "siteGroup", None)
            if site_group and getattr(site_group, "id", None):
                group_ids.add(site_group.id)

            if user_ids or group_ids:
                break

        # Link-based grants (shared links granted to multiple identities)
        for identities_attr in ("granted_to_identities_v2", "granted_to_identities"):
            identities_collection = getattr(permission, identities_attr, None)
            if not identities_collection:
                continue
            for identity_set in identities_collection:
                user = getattr(identity_set, "user", None)
                if user and getattr(user, "id", None):
                    user_ids.add(user.id)
                site_user = getattr(identity_set, "siteUser", None)
                if site_user and getattr(site_user, "id", None):
                    user_ids.add(site_user.id)
                group = getattr(identity_set, "group", None)
                if group and getattr(group, "id", None):
                    group_ids.add(group.id)
                site_group = getattr(identity_set, "siteGroup", None)
                if site_group and getattr(site_group, "id", None):
                    group_ids.add(site_group.id)

        return user_ids, group_ids

    def extract_permissions(
        self, permissions: list["Permission"]
    ) -> Optional[list[dict[str, Any]]]:
        """Normalize Microsoft Graph permissions to the standard format.

        Returns the same structure as Google Drive and Confluence connectors:
        [{"read": {"users": [...], "groups": [...]}},
         {"update": {"users": [...], "groups": [...]}},
         {"delete": {"users": [...], "groups": [...]}}]
        """
        if not permissions:
            logger.debug("no permissions found")
            return [{}]

        normalized: dict[str, dict[str, set[str]]] = {
            "read": {"users": set(), "groups": set()},
            "update": {"users": set(), "groups": set()},
            "delete": {"users": set(), "groups": set()},
        }

        for perm in permissions:
            roles = list(perm.roles) if perm.roles else []
            operations: set[str] = set()
            for role in roles:
                mapped = MICROSOFT_ROLE_MAPPING.get(role.lower(), [])
                operations.update(mapped)
                if not mapped and role.lower() not in MICROSOFT_ROLE_MAPPING:
                    logger.debug(f"unmapped SharePoint permission role: {role}")

            if not operations:
                continue

            user_ids, group_ids = self._extract_identity_ids(perm)

            for op in operations:
                normalized[op]["users"].update(user_ids)
                normalized[op]["groups"].update(group_ids)

        # Convert sets to sorted lists for consistency and JSON serialization
        result: dict[str, dict[str, list[str]]] = {}
        for op, principals in normalized.items():
            result[op] = {k: sorted(v) for k, v in principals.items()}

        logger.debug(f"normalized permissions generated: {result}")
        return [{k: v} for k, v in result.items()]

    def drive_item_to_file_data_sync(self, drive_item: "DriveItem") -> FileData:
        file_data = super().drive_item_to_file_data_sync(drive_item)
        try:
            permissions = drive_item.permissions.get().execute_query()
            file_data.metadata.permissions_data = self.extract_permissions(
                list(permissions)
            )
        except Exception as e:
            logger.warning(
                f"Failed to fetch permissions for {drive_item.name}: {e}"
            )
        return file_data

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
            site = client.sites.get_by_url(self.connection_config.site).get().execute_query()
            site_drive_item = site.drive.get().execute_query().root
        except ClientRequestException:
            logger.info("Site not found")

        path = self.index_config.path
        # Deprecated sharepoint sdk needed a default path. Microsoft Graph SDK does not.
        if path and path != LEGACY_DEFAULT_PATH:
            site_drive_item = site_drive_item.get_by_path(path).get().execute_query()

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
    @requires_dependencies(["office365"], extras="sharepoint")
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
