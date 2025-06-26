from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Generator, List, Optional, Tuple

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.errors_v2 import UserAuthError, UserError
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


class ConfluenceConnectionConfig(ConnectionConfig):
    url: str = Field(description="URL of the Confluence instance")
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
        if self.cloud and not basic_auth:
            raise ValueError(
                "cloud authentication requires username and API token (--password), "
                "see: https://atlassian-python-api.readthedocs.io/"
            )
        if basic_auth and pat_auth:
            raise ValueError(
                "both password and token provided, only one allowed, "
                "see: https://atlassian-python-api.readthedocs.io/"
            )
        if not (basic_auth or pat_auth):
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

    @requires_dependencies(["atlassian"], extras="confluence")
    @contextmanager
    def get_client(self) -> Generator["Confluence", None, None]:
        from atlassian import Confluence

        access_configs = self.access_config.get_secret_value()
        with Confluence(
            url=self.url,
            username=self.username,
            password=self.password_or_api_token(),
            token=access_configs.token,
            cloud=self.cloud,
        ) as client:
            yield client


class ConfluenceIndexerConfig(IndexerConfig):
    max_num_of_spaces: int = Field(500, description="Maximum number of spaces to index")
    max_num_of_docs_from_each_space: int = Field(
        100, description="Maximum number of documents to fetch from each space"
    )
    spaces: Optional[List[str]] = Field(None, description="List of specific space keys to index")


@dataclass
class ConfluenceIndexer(Indexer):
    connection_config: ConfluenceConnectionConfig
    index_config: ConfluenceIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> bool:
        try:
            self.connection_config.get_client()
        except Exception as e:
            logger.exception(f"Failed to connect to Confluence: {e}")
            raise UserAuthError(f"Failed to connect to Confluence: {e}")

        with self.connection_config.get_client() as client:
            # opportunistically check the first space in list of all spaces
            try:
                client.get_all_spaces(limit=1)
            except Exception as e:
                logger.exception(f"Failed to connect to find any Confluence space: {e}")
                raise UserError(f"Failed to connect to find any Confluence space: {e}")

            logger.info("Connection to Confluence successful.")

            # If specific spaces are provided, check if we can access them
            errors = []

            if self.index_config.spaces:
                for space_key in self.index_config.spaces:
                    try:
                        client.get_space(space_key)
                    except Exception as e:
                        logger.exception(f"Failed to connect to Confluence: {e}")
                        errors.append(f"Failed to connect to '{space_key}' space, cause: '{e}'")

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
                    space = client.get_space(space_key)
                    space_ids_and_keys.append((space_key, space["id"]))
                return space_ids_and_keys
        else:
            with self.connection_config.get_client() as client:
                all_spaces = client.get_all_spaces(limit=self.index_config.max_num_of_spaces)
            space_ids_and_keys = [(space["key"], space["id"]) for space in all_spaces["results"]]
            return space_ids_and_keys

    def _get_docs_ids_within_one_space(self, space_key: str) -> List[dict]:
        with self.connection_config.get_client() as client:
            pages = client.get_all_pages_from_space(
                space=space_key,
                start=0,
                expand=None,
                content_type="page",  # blogpost and comment types not currently supported
                status=None,
            )
        # Limit the number of documents to max_num_of_docs_from_each_space
        # Note: this is needed because the limit field in client.get_all_pages_from_space does
        # not seem to work as expected
        limited_pages = pages[: self.index_config.max_num_of_docs_from_each_space]
        doc_ids = [{"space_id": space_key, "doc_id": page["id"]} for page in limited_pages]
        return doc_ids

    def run(self) -> Generator[FileData, None, None]:
        from time import time

        space_ids_and_keys = self._get_space_ids_and_keys()
        for space_key, space_id in space_ids_and_keys:
            doc_ids = self._get_docs_ids_within_one_space(space_key)
            for doc in doc_ids:
                doc_id = doc["doc_id"]
                # Build metadata
                metadata = FileDataSourceMetadata(
                    date_processed=str(time()),
                    url=f"{self.connection_config.url}/pages/{doc_id}",
                    record_locator={
                        "space_id": space_key,
                        "document_id": doc_id,
                    },
                )
                additional_metadata = {
                    "space_key": space_key,
                    "space_id": space_id,  # diff from record_locator space_id (which is space_key)
                    "document_id": doc_id,
                }

                # Construct relative path and filename
                filename = f"{doc_id}.html"
                relative_path = str(Path(space_key) / filename)

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
                page = client.get_page_by_id(
                    page_id=doc_id,
                    expand="history.lastUpdated,version,body.view",
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
        file_data.metadata.date_created = page["history"]["createdDate"]
        file_data.metadata.date_modified = page["version"]["when"]
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
)
