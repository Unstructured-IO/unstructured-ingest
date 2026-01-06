from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncIterator, Literal, Optional

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
    from office365.sharepoint.client_context import ClientContext

CONNECTOR_TYPE = "sharepoint"
LEGACY_DEFAULT_PATH = "Shared Documents"


class SharepointAccessConfig(OnedriveAccessConfig):
    client_cred: str = Field(description="Microsoft App client secret")


class SharepointConnectionConfig(OnedriveConnectionConfig):
    deployment_type: Literal["online", "onpremise"] = Field(
        default="online",
        description="SharePoint deployment type: 'online' for SharePoint Online "
        "(*.sharepoint.com) or 'onpremise' for on-premise SharePoint servers",
    )
    auth_method: Literal["graph_api", "user_credential", "client_credential", "ntlm"] = Field(
        default="graph_api",
        description="Authentication method: 'graph_api' for SharePoint Online, "
        "'user_credential' or 'client_credential' or 'ntlm' for on-premise",
    )
    allow_ntlm: bool = Field(
        default=False,
        description="Enable NTLM authentication (only for on-premise deployments)",
    )
    user_pname: Optional[str] = Field(
        default=None,
        description="User principal name or service account, usually your Azure AD email "
        "for online, or DOMAIN\\username for on-premise.",
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

    @requires_dependencies(["office365"], extras="sharepoint")
    def get_sharepoint_client(self) -> "ClientContext":
        """
        Creates ClientContext for on-premise SharePoint.
        Routes to appropriate auth method based on self.auth_method.
        """
        from office365.sharepoint.client_context import ClientContext

        ctx = ClientContext(self.site)

        username = self.user_pname
        password = self.access_config.get_secret_value().client_cred

        if self.auth_method == "user_credential":
            # Standard username/password auth
            ctx = ctx.with_user_credentials(username, password)
        elif self.auth_method == "client_credential":
            # App-only auth with client credentials
            client_id = self.client_id
            client_secret = self.access_config.get_secret_value().client_cred
            ctx = ctx.with_client_credentials(client_id, client_secret)
        elif self.auth_method == "ntlm":
            # NTLM auth (Windows integrated)
            if self.allow_ntlm:
                ctx = ctx.with_user_credentials(username, password, allow_ntlm=True)
            else:
                raise UserError(
                    "NTLM authentication requires allow_ntlm=True to be explicitly set"
                )
        else:
            raise UserError(
                f"Unsupported auth method '{self.auth_method}' for on-premise SharePoint"
            )

        return ctx

    @requires_dependencies(["office365"], extras="sharepoint")
    def get_site(self) -> "Site":
        """
        Gets SharePoint site - works for both online and on-premise.
        Routes based on self.deployment_type.
        """
        if self.deployment_type == "onpremise":
            # On-premise: use ClientContext directly
            ctx = self.get_sharepoint_client()
            site = ctx.web.get().execute_query()
            return site
        else:
            # Online: use existing Graph API logic
            client = self.get_client()
            site = client.sites.get_by_url(self.site).get().execute_query()
            return site


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

        try:
            if self.connection_config.deployment_type == "onpremise":
                # On-premise: validate connection using ClientContext
                ctx = self.connection_config.get_sharepoint_client()
                site = ctx.web.get().execute_query()
                logger.info(f"On-premise SharePoint connection validated: {site.url}")
            else:
                # Online: validate authentication using Graph API
                self.connection_config.get_token()
                client = self.connection_config.get_client()
                client_site = (
                    client.sites.get_by_url(self.connection_config.site).get().execute_query()
                )
                site_drive_item = self.connection_config._get_drive_item(client_site)

                path = self.index_config.path
                if not self._is_root_path(path):
                    self._validate_folder_path(site_drive_item, path)

                logger.info(
                    f"SharePoint Online connection validated successfully for site: "
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

        if self.connection_config.deployment_type == "onpremise":
            # On-premise: use ClientContext and SharePoint REST API
            ctx = await asyncio.to_thread(self.connection_config.get_sharepoint_client)
            web = ctx.web.get().execute_query()

            # Get folder path
            path = self.index_config.path or ""
            if path and path != LEGACY_DEFAULT_PATH:
                folder = web.get_folder_by_server_relative_url(path)
            else:
                # Default to Shared Documents
                folder = web.default_document_library().root_folder

            # Get files from folder
            try:
                files = folder.files if not self.index_config.recursive else folder.get_files(
                    recursive=True
                )
                files.execute_query()

                for file in files:
                    # Convert on-premise file to FileData
                    file_data = FileData(
                        identifier=file.properties["ServerRelativeUrl"],
                        connector_type=self.connector_type,
                        source_identifiers=SourceIdentifiers(
                            fullpath=file.properties["ServerRelativeUrl"],
                            filename=file.properties["Name"],
                        ),
                        metadata=FileDataSourceMetadata(
                            date_modified=str(file.properties.get("TimeLastModified")),
                            date_created=str(file.properties.get("TimeCreated")),
                            version=str(file.properties.get("UIVersion")),
                        ),
                    )
                    yield file_data
            except ClientRequestException as e:
                logger.error(f"Failed to access SharePoint folder: {path}")
                raise SourceConnectionError(
                    f"Unable to access SharePoint folder at {path}: {str(e)}"
                )
        else:
            # Online: use existing Graph API logic
            token_resp = await asyncio.to_thread(self.connection_config.get_token)
            if "error" in token_resp:
                raise SourceConnectionError(
                    f"[{self.connector_type}]: {token_resp['error']} "
                    f"({token_resp.get('error_description')})"
                )

            client = await asyncio.to_thread(self.connection_config.get_client)
            try:
                client_site = (
                    client.sites.get_by_url(self.connection_config.site).get().execute_query()
                )
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

        if self.connection_config.deployment_type == "onpremise":
            # On-premise: use ClientContext to fetch file
            @retry(
                stop=stop_after_attempt(self.download_config.max_retries),
                wait=wait_exponential(exp_base=2, multiplier=1, min=2, max=10),
                retry=retry_if_exception(self.retry_on_status_code),
                before=before_log(logger, logging.DEBUG),
                reraise=True,
            )
            def _get_onpremise_file():
                try:
                    ctx = self.connection_config.get_sharepoint_client()
                    file = ctx.web.get_file_by_server_relative_url(
                        server_relative_path
                    ).get().execute_query()
                    return file
                except ClientRequestException:
                    logger.info(f"File not found: {server_relative_path}")
                    raise NotFoundError(f"File not found: {server_relative_path}")

            file = _get_onpremise_file()
        else:
            # Online: use Graph API
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
