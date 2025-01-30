from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Generator, List, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.html import HtmlMixin
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
    download_responses,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    SourceRegistryEntry,
)

if TYPE_CHECKING:
    from atlassian import Confluence

CONNECTOR_TYPE = "confluence"


class ConfluenceAccessConfig(AccessConfig):
    password: Optional[str] = Field(
        description="Confluence password or Cloud API token",
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
        basic_auth = self.username and access_configs.password
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

    @requires_dependencies(["atlassian"], extras="confluence")
    @contextmanager
    def get_client(self) -> "Confluence":
        from atlassian import Confluence

        access_configs = self.access_config.get_secret_value()
        with Confluence(
            url=self.url,
            username=self.username,
            password=access_configs.password,
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

    def _get_space_ids(self) -> List[str]:
        spaces = self.index_config.spaces
        if spaces:
            return spaces
        else:
            with self.connection_config.get_client() as client:
                all_spaces = client.get_all_spaces(limit=self.index_config.max_num_of_spaces)
            space_ids = [space["key"] for space in all_spaces["results"]]
            return space_ids

    def _get_docs_ids_within_one_space(self, space_id: str) -> List[dict]:
        with self.connection_config.get_client() as client:
            pages = client.get_all_pages_from_space(
                space=space_id,
                start=0,
                limit=self.index_config.max_num_of_docs_from_each_space,
                expand=None,
                content_type="page",
                status=None,
            )
        doc_ids = [{"space_id": space_id, "doc_id": page["id"]} for page in pages]
        return doc_ids

    def run(self) -> Generator[FileData, None, None]:
        from time import time

        space_ids = self._get_space_ids()
        for space_id in space_ids:
            doc_ids = self._get_docs_ids_within_one_space(space_id)
            for doc in doc_ids:
                doc_id = doc["doc_id"]
                # Build metadata
                metadata = FileDataSourceMetadata(
                    date_processed=str(time()),
                    url=f"{self.connection_config.url}/pages/{doc_id}",
                    record_locator={
                        "space_id": space_id,
                        "document_id": doc_id,
                    },
                )
                additional_metadata = {
                    "space_id": space_id,
                    "document_id": doc_id,
                }

                # Construct relative path and filename
                filename = f"{doc_id}.html"
                relative_path = str(Path(space_id) / filename)

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
        # This supports v2 html parsing in unstructured
        title = page["title"]
        title_html = f"<title>{title}</title>"
        content = f"<body class='Document' >{title_html}{content}</body>"
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
