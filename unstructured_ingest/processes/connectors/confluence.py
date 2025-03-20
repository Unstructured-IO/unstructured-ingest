from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Generator, List, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError
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
    def get_client(self) -> "Confluence":
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
            # Attempt to retrieve a list of spaces with limit=1.
            # This should only succeed if all creds are valid
            with self.connection_config.get_client() as client:
                client.get_all_spaces(limit=1)
            logger.info("Connection to Confluence successful.")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Confluence: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to connect to Confluence: {e}")

    def _get_space_ids_and_keys(self): # -> List[(str, int)]:
        """
        Get a list of space IDs and keys from Confluence.
        Example space ID (int): 98503 
        Example space key (str): "SD"
        """
        spaces = self.index_config.spaces
        if spaces:
            return spaces
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
                limit=self.index_config.max_num_of_docs_from_each_space,
                expand=None,
                content_type="page", # blogpost and comment types not currently supported
                status=None,
            )
        doc_ids = [{"space_id": space_key, "doc_id": page["id"]} for page in pages]
        return doc_ids

    def _get_permissions_for_doc(self, space_id: int, doc_id: str, space_key: str) -> Optional[List[dict]]:
        from requests.exceptions import HTTPError
        
        def parse_permissions(doc_permissions: dict, space_permissions: list):
            # space: list of SpacePermissionAssignment
            # doc: ContentRestrictionArray
            
            # get doc perms. if they exist, they will override space level, and only delete comes from space
            # if they dont exist, use space perms for read and write
            # if space perms dont exist, use default perms for read and write

            normalized_perms_dict = {}

            # flag for if page restrictions are enabled
            page_restrictions_found = any(
                permissions.get("restrictions", {}).get("user", {}).get("results") or
                permissions.get("restrictions", {}).get("group", {}).get("results")
                for permissions in doc_permissions.values()
            )

            if page_restrictions_found:
                for action, permissions in doc_permissions.items():
                    restrictions_dict = permissions.get("restrictions", {})

                    for entity_type, entity_data in restrictions_dict.items():
                        for entity in entity_data.get("results"):
                            if entity_type == "user":
                                entity_id = entity["accountId"]
                            elif entity_type == "group":
                                entity_id = entity["id"]

                            print(f"DOC id {entity_id} with action {action}")
                            doc_perm_entry = normalized_perms_dict.setdefault(
                                entity_id,
                                {"type": entity_type, "id": entity_id, "email_address": entity.get("email", None), "role": set()},
                            )
                            doc_perm_entry["role"].add(action)

            
            for space_perm in space_permissions:
                space_operation = space_perm["operation"]["key"]
                space_target_type = space_perm["operation"]["targetType"]

                if space_target_type == "space":
                    space_entity_id = space_perm["principal"]["id"]
                    if not page_restrictions_found:
                        if space_operation in {"read", "administer"}:
                            print("SPACE SPERM: ", space_perm)
                            role = {"read"} if space_operation == "read" else {"read", "update"}
                            print(f"SPACE id {space_entity_id} with action {role}")
                            perm_entry = normalized_perms_dict.setdefault(
                                space_entity_id, {"type": space_perm["principal"]["type"], "id": space_entity_id, "email_address": None, "role": set()}
                            )
                            perm_entry["role"].update(role)
                
                # always add "delete page" space permissions
                elif space_target_type == "page" and space_operation == "delete":
                    print("SPACE PDPERM: ", space_perm)
                    space_entity_id = space_perm["principal"]["id"]
                    print(f"SPACED id {space_entity_id} with delete")
                    perm_entry = normalized_perms_dict.setdefault(
                        space_entity_id, {"type": space_perm["principal"]["type"], "id": space_entity_id, "email_address": None, "role": set()}
                    )
                    perm_entry["role"].add("delete")

                # if space_perm["operation"]["targetType"] == "space":
                #     print("SPACE PSPERM: ", space_perm)
                #     relevant_space_permissions = ["read", "administer"]
                #     if (space_operation := space_perm["operation"]["key"]) in relevant_space_permissions:
                #         space_entity_id = space_perm["principal"]["id"]
                #         if space_entity_id in normalized_perms_dict:
                #             print(f"SPACE update id {space_entity_id} with action {space_operation}")
                #             normalized_perms_dict[space_entity_id]["role"].update(space_operation)
                #         else:
                #             # Create a new entry
                #             print(f"SPACE create id {space_entity_id} with action {space_operation}")
                #             normalized_perms_dict[space_entity_id] = {
                #                 "type": space_perm["principal"]["type"],
                #                 "id": space_entity_id,
                #                 "email_address": None,
                #                 "role": space_operation,
                #             }
                
            # # merge in "delete page" space permissions
            # for space_perm in space_permissions:
            #     space_operation = "delete"
            #     if space_perm["operation"]["key"] == space_operation:
            #         space_entity_id = space_perm["principal"]["id"]
            #         if space_entity_id in normalized_perms_dict:
            #             print(f"SPACED update id {space_entity_id} w delete")
            #             normalized_perms_dict[space_entity_id]["role"].add(space_operation)
            #         else:
            #             print(f"SPACED create id {space_entity_id} w delete")
            #             normalized_perms_dict[space_entity_id] = {
            #                 "type": space_perm["principal"]["type"],
            #                 "id": space_entity_id,
            #                 "email_address": None,
            #                 "role": set([space_operation]),
            #             }



            
            
            # for space_perm in space_permissions:
            #     if space_perm["operation"]["targetType"] in ["page", "space"]:
            #         print("SPACE PSPERM: ", space_perm)
            #         # if page restrictions are not enabled, use all space permissions
            #         if not page_restrictions_found:
            #             relevant_space_permissions = ["read", "administer", "delete"]
            #             if (space_operation := space_perm["operation"]["key"]) in relevant_space_permissions:
            #                 space_entity_id = space_perm["principal"]["id"]
            #                 space_operation = {space_operation}
            #                 if space_operation == {"administer"}:
            #                     space_operation = {"read", "update"}
            #                 if space_entity_id in normalized_perms_dict:
            #                     print(f"SPACE update id {space_entity_id} with action {space_operation}")
            #                     normalized_perms_dict[space_entity_id]["role"].update(space_operation)
            #                 else:
            #                     # Create a new entry
            #                     print(f"SPACE create id {space_entity_id} with action {space_operation}")
            #                     normalized_perms_dict[space_entity_id] = {
            #                         "type": space_perm["principal"]["type"],
            #                         "id": space_entity_id,
            #                         "email_address": None,
            #                         "role": space_operation,
            #                     }
            #         # add space level delete info
            #         else: 
            #             space_operation = "delete"
            #             if space_perm["operation"]["key"] == space_operation:
            #                 space_entity_id = space_perm["principal"]["id"]
            #                 if space_entity_id in normalized_perms_dict:
            #                     print(f"SPACED update id {space_entity_id} w delete")
            #                     normalized_perms_dict[space_entity_id]["role"].add(space_operation)
            #                 else:
            #                     print(f"SPACED create id {space_entity_id} w delete")
            #                     normalized_perms_dict[space_entity_id] = {
            #                         "type": space_perm["principal"]["type"],
            #                         "id": space_entity_id,
            #                         "email_address": None,
            #                         "role": set([space_operation]),
            #                     }
            
            # turn sets into sorted lists for consistency and json serialization
            for perm_data in normalized_perms_dict.values():
                perm_data["role"] = sorted(list(perm_data["role"]))
            
            return list(normalized_perms_dict.values())
        
        
        with self.connection_config.get_client() as client:
            try:
                print("ENTERED TRY")
                # space permissions
                space_permissions = []
                space_permissions_result = client.get(f"/api/v2/spaces/{space_id}/permissions")
                space_permissions.extend(space_permissions_result["results"])
                if space_permissions_result["_links"].get("next"): # pagination
                    print("got a next")
                    while space_permissions_result.get("next"):
                        space_permissions_result = client.get(space_permissions_result["next"])
                        space_permissions.extend(space_permissions_result["results"])
                
                print(f"GOT space_permissions for id {space_id} key {space_key}: ")
                for space_perm in space_permissions:
                    if space_perm["operation"]["targetType"] in ["page", "space"]:
                        print("OSPACE PSPERM: ", space_perm)
                # check for read or administer space -> page read
                # check for administer space -> page write

                doc_permissions = client.get_all_restrictions_for_content(content_id=doc_id)
                print(f"GOT doc_permissions for doc {doc_id}: ", doc_permissions)

                parsed_perms = parse_permissions(doc_permissions, space_permissions)
                print("PARSED DOC PERMS: ", parsed_perms)


            except HTTPError as e:
                # skip writing any permission metadata
                logger.debug(f"Could not retrieve permissions for doc {doc_id} in space {space_id}: {e}")
                return None
      
        # TODO adjust permissions_data FileDataSourceMetadata type to match
        return parsed_perms


    def run(self) -> Generator[FileData, None, None]:
        from time import time

        space_ids_and_keys = self._get_space_ids_and_keys()
        # for space_id in space_ids:
        # for space_id, space_id_real in zip(space_ids, space_ids_real):
        for space_key, space_id in space_ids_and_keys:
            doc_ids = self._get_docs_ids_within_one_space(space_key)
            for doc in doc_ids:
                doc_id = doc["doc_id"]
                # Build metadata
                # print(self._get_permissions_for_doc(space_id, doc_id), "runperms")
                metadata = FileDataSourceMetadata(
                    date_processed=str(time()),
                    url=f"{self.connection_config.url}/pages/{doc_id}",
                    permissions_data=self._get_permissions_for_doc(space_id, doc_id, space_key),
                    record_locator={
                        "space_id": space_key,
                        "document_id": doc_id,
                    },
                )
                additional_metadata = {
                    "space_id": space_key,
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
                )
                yield file_data


class ConfluenceDownloaderConfig(DownloaderConfig, HtmlMixin):
    pass


@dataclass
class ConfluenceDownloader(Downloader):
    connection_config: ConfluenceConnectionConfig
    download_config: ConfluenceDownloaderConfig = field(default_factory=ConfluenceDownloaderConfig)
    connector_type: str = CONNECTOR_TYPE

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
            logger.error(f"Failed to retrieve page with ID {doc_id}: {e}", exc_info=True)
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
