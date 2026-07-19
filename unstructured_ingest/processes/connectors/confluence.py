from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Generator, List, Optional, Tuple
from urllib.parse import urlparse

from dateutil import parser
from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import (
    SourceConnectionError,
    UnstructuredIngestError,
    UserAuthError,
    UserError,
    ValueError,
    safe_error_summary,
)
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
    download_responses,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    LocationShape,
    SourceRegistryEntry,
)
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.html import HtmlMixin
from unstructured_ingest.utils.string_and_date_utils import fix_unescaped_unicode

if TYPE_CHECKING:
    from atlassian import Confluence
    from bs4 import BeautifulSoup
    from bs4.element import Tag

CONNECTOR_TYPE = "confluence"
CONFLUENCE_SPACE_PAGE_SIZE = 250
CONFLUENCE_PAGE_PAGE_SIZE = 250


def _iso8601_to_epoch_str(iso_date: Optional[str]) -> Optional[str]:
    """Convert an ISO8601 datetime string to a Unix epoch string for FileData metadata."""
    if iso_date is None:
        return None
    return str(parser.parse(iso_date).timestamp())


class ConfluenceAccessConfig(AccessConfig):
    password: Optional[str] = Field(
        description="Confluence password",
        default=None,
    )
    api_token: Optional[str] = Field(
        description="Confluence Cloud API token",
        default=None,
    )
    token: Optional[str] = Field(
        description="Confluence Personal Access Token",
        default=None,
    )
    oauth_token: Optional[str] = Field(
        description="Atlassian OAuth 2.0 access token",
        default=None,
    )
    refresh_token: Optional[str] = Field(
        description=(
            "Atlassian OAuth 2.0 refresh token. Used by the platform to refresh expired "
            "access tokens before each job run."
        ),
        default=None,
    )


class ConfluenceConnectionConfig(ConnectionConfig):
    url: str = Field(
        description="URL of the Confluence instance",
        json_schema_extra={"x-runtime-eligible": True},
    )
    cloud_id: Optional[str] = Field(
        description="Atlassian Cloud ID for OAuth 2.0 API gateway requests",
        default=None,
    )
    username: Optional[str] = Field(
        description="Username or email for authentication",
        default=None,
    )
    cloud: bool = Field(description="Authenticate to Confluence Cloud", default=False)
    access_config: Secret[ConfluenceAccessConfig] = Field(
        description="Access configuration for Confluence"
    )

    def model_post_init(self, __context):
        access_configs = self.access_config.get_secret_value()
        if access_configs.password and access_configs.api_token:
            raise ValueError(
                "both password and api_token provided, only one allowed, "
                "see: https://atlassian-python-api.readthedocs.io/"
            )
        basic_auth = bool(self.username and (access_configs.password or access_configs.api_token))
        pat_auth = access_configs.token
        oauth_auth = bool(access_configs.oauth_token)
        if self.cloud and not (basic_auth or oauth_auth):
            raise ValueError(
                "cloud authentication requires username and API token (--password) or oauth_token, "
                "see: https://atlassian-python-api.readthedocs.io/"
            )
        auth_methods = [basic_auth, bool(pat_auth), oauth_auth]
        if sum(bool(method) for method in auth_methods) > 1:
            raise ValueError(
                "basic auth, pat token, and oauth token are mutually exclusive, "
                "see: https://atlassian-python-api.readthedocs.io/"
            )
        if oauth_auth and not self.cloud_id:
            raise ValueError("cloud_id is required when using oauth_token")
        if not any(auth_methods):
            raise ValueError(
                "no form of auth provided, see: https://atlassian-python-api.readthedocs.io/"
            )

    def password_or_api_token(self) -> str:
        # Confluence takes either password or API token under the same field: password
        # This ambiguity led to confusion, so we are making it specific what you are passing in
        access_configs = self.access_config.get_secret_value()
        if access_configs.password:
            return access_configs.password
        return access_configs.api_token

    def api_url(self) -> str:
        if self.access_config.get_secret_value().oauth_token:
            return f"https://api.atlassian.com/ex/confluence/{self.cloud_id}/wiki"
        return self.url

    def page_url(self, page_id: str) -> str:
        return f"{self.url.rstrip('/')}/pages/{page_id}"

    @requires_dependencies(["atlassian"], extras="confluence")
    @contextmanager
    def get_client(self) -> Generator["Confluence", None, None]:
        from atlassian import Confluence

        access_configs = self.access_config.get_secret_value()
        with Confluence(
            url=self.api_url(),
            username=None if access_configs.oauth_token else self.username,
            password=None if access_configs.oauth_token else self.password_or_api_token(),
            token=access_configs.token,
            cloud=self.cloud or bool(access_configs.oauth_token),
        ) as client:
            if access_configs.oauth_token:
                client._session.headers.update(
                    {"Authorization": f"Bearer {access_configs.oauth_token}"}
                )
            yield client


class ConfluenceIndexerConfig(IndexerConfig):
    max_num_of_spaces: int = Field(500, description="Maximum number of spaces to index")
    max_num_of_docs_from_each_space: int = Field(
        100, description="Maximum number of documents to fetch from each space"
    )
    spaces: Optional[List[str]] = Field(
        None,
        description="List of specific space keys to index",
        json_schema_extra={"x-runtime-eligible": True},
    )


@dataclass
class ConfluenceIndexer(Indexer):
    connection_config: ConfluenceConnectionConfig
    index_config: ConfluenceIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    @staticmethod
    def _get_next_page_path(response: dict) -> Optional[str]:
        next_link = response.get("_links", {}).get("next")
        if not next_link:
            return None

        parsed_next_link = urlparse(next_link)
        next_path = parsed_next_link.path or next_link
        if "/wiki/" in next_path:
            next_path = next_path.split("/wiki/", maxsplit=1)[1]
        else:
            next_path = next_path.lstrip("/")
        if parsed_next_link.query:
            next_path = f"{next_path}?{parsed_next_link.query}"
        return next_path

    def _paginate_v2_results(
        self,
        client: "Confluence",
        *,
        path: str,
        params: dict,
        limit: int,
    ) -> List[dict]:
        results: List[dict] = []
        next_path: Optional[str] = path
        next_params: Optional[dict] = params
        while next_path and len(results) < limit:
            response = client.get(next_path, params=next_params)
            remaining = limit - len(results)
            results.extend(response.get("results", [])[:remaining])
            next_path = self._get_next_page_path(response)
            next_params = None
        return results

    def _list_spaces(
        self,
        client: "Confluence",
        *,
        keys: Optional[List[str]] = None,
        limit: Optional[int] = None,
        space_type: Optional[str] = None,
    ) -> List[dict]:
        target_limit = limit or self.index_config.max_num_of_spaces
        params: dict = {
            "limit": min(target_limit, CONFLUENCE_SPACE_PAGE_SIZE),
        }
        if keys:
            params["keys"] = keys
        if space_type:
            params["type"] = space_type
            params["status"] = "current"
        return self._paginate_v2_results(
            client,
            path="api/v2/spaces",
            params=params,
            limit=target_limit,
        )

    @staticmethod
    def _space_matches_key(space: dict, space_key: str) -> bool:
        return space.get("key") == space_key or space.get("alias") == space_key

    def _get_space_by_key(self, client: "Confluence", space_key: str) -> dict:
        for space in self._list_spaces(client, keys=[space_key], limit=1):
            if self._space_matches_key(space, space_key):
                return space
        if space_key.startswith("~"):
            # Personal space aliases are not reliably returned by the v2 keys
            # filter, so fall back to scanning current personal spaces.
            for space in self._list_spaces(client, space_type="personal"):
                if self._space_matches_key(space, space_key):
                    return space
        raise UserError(f"Failed to find '{space_key}' space")

    def precheck(self) -> bool:
        try:
            self.connection_config.get_client()
        except (ImportError, UnstructuredIngestError):
            # Preserve dependency-install guidance and connector-authored typed
            # errors; only unexpected exceptions are redacted below.
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Confluence: {safe_error_summary(e)}")
            raise UserAuthError(
                f"Failed to connect to Confluence: {safe_error_summary(e)}"
            ) from None

        with self.connection_config.get_client() as client:
            # opportunistically check the first space in list of all spaces
            try:
                self._list_spaces(client, limit=1)
            except (ImportError, UnstructuredIngestError):
                # Preserve dependency-install guidance and connector-authored
                # typed errors; only unexpected exceptions are redacted below.
                raise
            except Exception as e:
                logger.error(
                    f"Failed to connect to find any Confluence space: {safe_error_summary(e)}"
                )
                raise UserError(
                    f"Failed to connect to find any Confluence space: {safe_error_summary(e)}"
                ) from None

            logger.info("Connection to Confluence successful.")

            # If specific spaces are provided, check if we can access them
            errors = []

            if self.index_config.spaces:
                for space_key in self.index_config.spaces:
                    try:
                        self._get_space_by_key(client, space_key)
                    except UnstructuredIngestError as e:
                        # Preserve connector-authored guidance from _get_space_by_key.
                        logger.error(f"Failed to connect to Confluence: {safe_error_summary(e)}")
                        errors.append(f"Failed to connect to '{space_key}' space, cause: '{e}'")
                    except Exception as e:
                        logger.error(f"Failed to connect to Confluence: {safe_error_summary(e)}")
                        errors.append(
                            f"Failed to connect to '{space_key}' space, "
                            f"cause: '{safe_error_summary(e)}'"
                        )

            if errors:
                raise UserError("\n".join(errors))

        return True

    def _get_space_ids_and_keys(self) -> List[Tuple[str, int]]:
        """
        Get a list of space IDs and keys from Confluence.

        Example space ID (numerical): 98503
        Example space key (str): "SD"
        """
        spaces = self.index_config.spaces
        if spaces:
            with self.connection_config.get_client() as client:
                space_ids_and_keys = []
                for space_key in spaces:
                    space = self._get_space_by_key(client, space_key)
                    space_ids_and_keys.append((space_key, space["id"]))
                return space_ids_and_keys
        else:
            with self.connection_config.get_client() as client:
                all_spaces = self._list_spaces(client)
            space_ids_and_keys = [(space["key"], space["id"]) for space in all_spaces]
            return space_ids_and_keys

    def _get_docs_ids_within_one_space(self, space_id: int) -> List[dict]:
        with self.connection_config.get_client() as client:
            pages = self._paginate_v2_results(
                client,
                path="api/v2/pages",
                params={
                    "space-id": space_id,
                    "limit": min(
                        self.index_config.max_num_of_docs_from_each_space,
                        CONFLUENCE_PAGE_PAGE_SIZE,
                    ),
                },
                limit=self.index_config.max_num_of_docs_from_each_space,
            )
        # The v2 /pages list returns each page's creation date and version metadata
        # (createdAt + version{createdAt, number}) by default. We carry them here so the
        # indexer can populate FileData.metadata at index time, which is what the platform
        # uses to detect new/modified records (FS-2105 creation date, FS-2107 change detection).
        doc_ids = []
        for page in pages:
            version = page.get("version") or {}
            doc_ids.append(
                {
                    "space_id": space_id,
                    "doc_id": page["id"],
                    "date_created": _iso8601_to_epoch_str(page.get("createdAt")),
                    "date_modified": _iso8601_to_epoch_str(version.get("createdAt")),
                    "version_number": version.get("number"),
                }
            )
        return doc_ids

    def run(self) -> Generator[FileData, None, None]:
        from time import time

        space_ids_and_keys = self._get_space_ids_and_keys()
        for space_key, space_id in space_ids_and_keys:
            doc_ids = self._get_docs_ids_within_one_space(space_id)
            for doc in doc_ids:
                doc_id = doc["doc_id"]
                version_number = doc.get("version_number")
                # Build metadata. date_created/date_modified/version come from the v2 /pages
                # list so the indexer reports them at index time; the platform relies on the
                # indexer's metadata to detect new/modified records (FS-2105 + FS-2107).
                metadata = FileDataSourceMetadata(
                    date_created=doc.get("date_created"),
                    date_modified=doc.get("date_modified"),
                    version=str(version_number) if version_number is not None else None,
                    date_processed=str(time()),
                    url=self.connection_config.page_url(doc_id),
                    record_locator={
                        "space_id": space_key,
                        "document_id": doc_id,
                        **(
                            {"cloud_id": self.connection_config.cloud_id}
                            if self.connection_config.cloud_id
                            else {}
                        ),
                    },
                )
                additional_metadata = {
                    "space_key": space_key,
                    "space_id": space_id,  # diff from record_locator space_id (which is space_key)
                    "document_id": doc_id,
                    **(
                        {
                            "cloud_id": self.connection_config.cloud_id,
                            "site_url": self.connection_config.url,
                        }
                        if self.connection_config.cloud_id
                        else {}
                    ),
                }

                # Construct relative path and filename
                filename = f"{doc_id}.html"
                path_parts = [space_key, filename]
                if self.connection_config.cloud_id:
                    path_parts.insert(0, self.connection_config.cloud_id)
                relative_path = str(Path(*path_parts))

                source_identifiers = SourceIdentifiers(
                    filename=filename,
                    fullpath=relative_path,
                    rel_path=relative_path,
                )

                file_data = FileData(
                    identifier=doc_id,
                    connector_type=self.connector_type,
                    metadata=metadata,
                    additional_metadata=additional_metadata,
                    source_identifiers=source_identifiers,
                    display_name=source_identifiers.fullpath,
                )
                yield file_data


class ConfluenceDownloaderConfig(HtmlMixin, DownloaderConfig):
    max_num_metadata_permissions: int = Field(
        250, description="Approximate maximum number of permissions included in metadata"
    )

    @requires_dependencies(["bs4"])
    def _find_hyperlink_tags(self, html_soup: "BeautifulSoup") -> list["Tag"]:
        from bs4.element import Tag

        return [
            element
            for element in html_soup.find_all(
                "a",
                attrs={
                    "class": "confluence-embedded-file",
                    "data-linked-resource-type": "attachment",
                    "href": True,
                },
            )
            if isinstance(element, Tag)
        ]


@dataclass
class ConfluenceDownloader(Downloader):
    connection_config: ConfluenceConnectionConfig
    download_config: ConfluenceDownloaderConfig = field(default_factory=ConfluenceDownloaderConfig)
    connector_type: str = CONNECTOR_TYPE
    _permissions_cache: dict = field(default_factory=OrderedDict)
    _permissions_cache_max_size: int = 5

    def download_embedded_files(
        self, session, html: str, current_file_data: FileData
    ) -> list[DownloadResponse]:
        if not self.download_config.extract_files:
            return []
        url = current_file_data.metadata.url
        if url is None:
            logger.warning(
                f"""Missing URL for file: {current_file_data.source_identifiers.filename}.
                Skipping file extraction."""
            )
            return []
        filepath = current_file_data.source_identifiers.relative_path
        download_path = Path(self.download_dir) / filepath
        download_dir = download_path.with_suffix("")
        return self.download_config.extract_embedded_files(
            url=url,
            download_dir=download_dir,
            original_filedata=current_file_data,
            html=html,
            session=session,
        )

    def parse_permissions(self, doc_permissions: dict, space_permissions: list) -> dict[str, dict]:
        """
        Parses document and space permissions to determine final user/group roles.

        :param doc_permissions: dict containing document-level restrictions
        - doc_permissions type in Confluence: ContentRestrictionArray
        :param space_permissions: list of space-level permission assignments
        - space_permissions type in Confluence: list of SpacePermissionAssignment
        :return: dict with operation as keys and each maps to dict with "users" and "groups"

        Get document permissions. If they exist, they will override space level permissions.
        Otherwise, apply relevant space permissions (read, administer, delete)
        """

        # Separate flags to track if view or edit is restricted at the page level
        page_view_restricted = bool(
            doc_permissions.get("read", {}).get("restrictions", {}).get("user", {}).get("results")
            or doc_permissions.get("read", {})
            .get("restrictions", {})
            .get("group", {})
            .get("results")
        )

        page_edit_restricted = bool(
            doc_permissions.get("update", {}).get("restrictions", {}).get("user", {}).get("results")
            or doc_permissions.get("update", {})
            .get("restrictions", {})
            .get("group", {})
            .get("results")
        )

        permissions_by_role = {
            "read": {"users": set(), "groups": set()},
            "update": {"users": set(), "groups": set()},
            "delete": {"users": set(), "groups": set()},
        }

        total_permissions = 0

        for action, permissions in doc_permissions.items():
            restrictions_dict = permissions.get("restrictions", {})

            for entity_type, entity_data in restrictions_dict.items():
                for entity in entity_data.get("results"):
                    entity_id = entity["accountId"] if entity_type == "user" else entity["id"]
                    permissions_by_role[action][f"{entity_type}s"].add(entity_id)
                    total_permissions += 1
                    # edit permission implies view permission
                    if action == "update":
                        permissions_by_role["read"][f"{entity_type}s"].add(entity_id)
                        # total_permissions += 1
                        # ^ omitting to not double count an entity.
                        # may result in a higher total count than max_num_metadata_permissions

        for space_perm in space_permissions:
            if total_permissions < self.download_config.max_num_metadata_permissions:
                space_operation = space_perm["operation"]["key"]
                space_target_type = space_perm["operation"]["targetType"]
                space_entity_id = space_perm["principal"]["id"]
                space_entity_type = space_perm["principal"]["type"]

                # Apply space-level view permissions if no page restrictions exist
                if (
                    space_target_type == "space"
                    and space_operation == "read"
                    and not page_view_restricted
                ):
                    permissions_by_role["read"][f"{space_entity_type}s"].add(space_entity_id)
                    total_permissions += 1

                # Administer permission includes view + edit. Apply if not page restricted
                elif space_target_type == "space" and space_operation == "administer":
                    if not page_view_restricted:
                        permissions_by_role["read"][f"{space_entity_type}s"].add(space_entity_id)
                        total_permissions += 1
                        if not page_edit_restricted:
                            permissions_by_role["update"][f"{space_entity_type}s"].add(
                                space_entity_id
                            )
                            # total_permissions += 1
                            # ^ omitting to not double count an entity.
                            # may result in a higher total count than max_num_metadata_permissions

                # Add the "delete page" space permissions if there are other page permissions
                elif (
                    space_target_type == "page"
                    and space_operation == "delete"
                    and space_entity_id in permissions_by_role["read"][f"{space_entity_type}s"]
                ):
                    permissions_by_role["delete"][f"{space_entity_type}s"].add(space_entity_id)
                    total_permissions += 1

        # turn sets into sorted lists for consistency and json serialization
        for role_dict in permissions_by_role.values():
            for key in role_dict:
                role_dict[key] = sorted(role_dict[key])

        return permissions_by_role

    def _get_permissions_for_space(self, space_id: int) -> Optional[List[dict]]:
        if space_id in self._permissions_cache:
            self._permissions_cache.move_to_end(space_id)  # mark recent use
            logger.debug(f"Retrieved cached permissions for space {space_id}")
            return self._permissions_cache[space_id]
        else:
            with self.connection_config.get_client() as client:
                try:
                    # TODO limit the total number of results being called.
                    # not yet implemented because this client call doesn't allow for filtering for
                    # certain operations, so adding a limit here would result in too little data.
                    space_permissions = []
                    space_permissions_result = client.get(f"/api/v2/spaces/{space_id}/permissions")
                    space_permissions.extend(space_permissions_result["results"])
                    if space_permissions_result["_links"].get("next"):  # pagination
                        while space_permissions_result.get("next"):
                            space_permissions_result = client.get(space_permissions_result["next"])
                            space_permissions.extend(space_permissions_result["results"])

                    if len(self._permissions_cache) >= self._permissions_cache_max_size:
                        self._permissions_cache.popitem(last=False)  # LRU/FIFO eviction
                    self._permissions_cache[space_id] = space_permissions

                    logger.debug(f"Retrieved permissions for space {space_id}")
                    return space_permissions
                except Exception as e:
                    logger.debug(f"Could not retrieve permissions for space {space_id}: {e}")
                    return None

    def _parse_permissions_for_doc(
        self, doc_id: str, space_permissions: list
    ) -> Optional[list[dict]]:
        with self.connection_config.get_client() as client:
            try:
                doc_permissions = client.get_all_restrictions_for_content(content_id=doc_id)
                parsed_permissions_dict = self.parse_permissions(doc_permissions, space_permissions)
                parsed_permissions_dict = [{k: v} for k, v in parsed_permissions_dict.items()]

            except Exception as e:
                # skip writing any permission metadata
                logger.debug(f"Could not retrieve permissions for doc {doc_id}: {e}")
                return None

        logger.debug(f"normalized permissions generated: {parsed_permissions_dict}")
        return parsed_permissions_dict

    def run(self, file_data: FileData, **kwargs) -> download_responses:
        from bs4 import BeautifulSoup

        doc_id = file_data.identifier
        try:
            with self.connection_config.get_client() as client:
                page = client.get(
                    f"api/v2/pages/{doc_id}",
                    params={
                        "body-format": "view",
                        "include-version": "true",
                    },
                )
        except Exception as e:
            logger.exception(f"Failed to retrieve page with ID {doc_id}: {e}")
            raise SourceConnectionError(f"Failed to retrieve page with ID {doc_id}: {e}")

        if not page:
            raise ValueError(f"Page with ID {doc_id} does not exist.")

        content = page["body"]["view"]["value"]
        title = page["title"]
        # Using h1 for title is supported by both v1 and v2 html parsing in unstructured
        title_html = f"<h1>{title}</h1>"
        content = fix_unescaped_unicode(f"<body class='Document' >{title_html}{content}</body>")
        if self.download_config.extract_images:
            with self.connection_config.get_client() as client:
                content = self.download_config.extract_html_images(
                    url=file_data.metadata.url, html=content, session=client._session
                )

        filepath = file_data.source_identifiers.relative_path
        download_path = Path(self.download_dir) / filepath
        download_path.parent.mkdir(parents=True, exist_ok=True)
        with open(download_path, "w", encoding="utf8") as f:
            soup = BeautifulSoup(content, "html.parser")
            f.write(soup.prettify())

        # Get document permissions and update metadata
        space_id = file_data.additional_metadata["space_id"]
        space_perm = self._get_permissions_for_space(space_id)  # must be the id, NOT the space key
        if space_perm:
            combined_doc_permissions = self._parse_permissions_for_doc(doc_id, space_perm)
            if combined_doc_permissions:
                file_data.metadata.permissions_data = combined_doc_permissions

        # Update file_data with metadata
        file_data.metadata.date_created = _iso8601_to_epoch_str(page["createdAt"])
        file_data.metadata.date_modified = _iso8601_to_epoch_str(page["version"]["createdAt"])
        file_data.metadata.version = str(page["version"]["number"])
        file_data.display_name = title

        download_response = self.generate_download_response(
            file_data=file_data, download_path=download_path
        )
        if self.download_config.extract_files:
            with self.connection_config.get_client() as client:
                extracted_download_responses = self.download_embedded_files(
                    html=content,
                    current_file_data=download_response["file_data"],
                    session=client._session,
                )
                if extracted_download_responses:
                    for dr in extracted_download_responses:
                        fd = dr["file_data"]
                        source_file_path = Path(file_data.source_identifiers.fullpath).with_suffix(
                            ""
                        )
                        new_fullpath = source_file_path / fd.source_identifiers.filename
                        fd.source_identifiers = SourceIdentifiers(
                            fullpath=new_fullpath.as_posix(), filename=new_fullpath.name
                        )
                    extracted_download_responses.append(download_response)
                    return extracted_download_responses
        return download_response


confluence_source_entry = SourceRegistryEntry(
    connection_config=ConfluenceConnectionConfig,
    indexer_config=ConfluenceIndexerConfig,
    indexer=ConfluenceIndexer,
    downloader_config=ConfluenceDownloaderConfig,
    downloader=ConfluenceDownloader,
    location_shape=LocationShape.API_FOLDER,
    location_identity=(
        "connector_config.url",
        "connector_config.cloud_id",
        "indexer_config.spaces",
    ),
    emits_record_version=True,
    supports_recursion=False,
)
