from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Generator, List, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.utils.dep_check import requires_dependencies
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
from unstructured_ingest.v2.processes.connector_registry import (
    SourceRegistryEntry,
)

if TYPE_CHECKING:
    from atlassian import Confluence

CONNECTOR_TYPE = "confluence"


class ConfluenceAccessConfig(AccessConfig):
    api_token: str = Field(description="Confluence API token")


class ConfluenceConnectionConfig(ConnectionConfig):
    url: str = Field(description="URL of the Confluence instance")
    user_email: str = Field(description="User email for authentication")
    access_config: Secret[ConfluenceAccessConfig] = Field(
        description="Access configuration for Confluence"
    )

    @requires_dependencies(["atlassian"], extras="confluence")
    def get_client(self) -> "Confluence":
        from atlassian import Confluence

        access_configs = self.access_config.get_secret_value()
        return Confluence(
            url=self.url,
            username=self.user_email,
            password=access_configs.api_token,
        )


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
            client = self.connection_config.get_client()
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
            client = self.connection_config.get_client()
            all_spaces = client.get_all_spaces(limit=self.index_config.max_num_of_spaces)
            space_ids = [space["key"] for space in all_spaces["results"]]
            return space_ids

    def _get_docs_ids_within_one_space(self, space_id: str) -> List[dict]:
        client = self.connection_config.get_client()
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


class ConfluenceDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class ConfluenceDownloader(Downloader):
    connection_config: ConfluenceConnectionConfig
    download_config: ConfluenceDownloaderConfig = field(default_factory=ConfluenceDownloaderConfig)
    connector_type: str = CONNECTOR_TYPE

    def run(self, file_data: FileData, **kwargs) -> DownloadResponse:
        doc_id = file_data.identifier
        try:
            client = self.connection_config.get_client()
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

        filepath = file_data.source_identifiers.relative_path
        download_path = Path(self.download_dir) / filepath
        download_path.parent.mkdir(parents=True, exist_ok=True)
        with open(download_path, "w", encoding="utf8") as f:
            f.write(content)

        # Update file_data with metadata
        file_data.metadata.date_created = page["history"]["createdDate"]
        file_data.metadata.date_modified = page["version"]["when"]
        file_data.metadata.version = str(page["version"]["number"])
        file_data.display_name = page["title"]

        return self.generate_download_response(file_data=file_data, download_path=download_path)


confluence_source_entry = SourceRegistryEntry(
    connection_config=ConfluenceConnectionConfig,
    indexer_config=ConfluenceIndexerConfig,
    indexer=ConfluenceIndexer,
    downloader_config=ConfluenceDownloaderConfig,
    downloader=ConfluenceDownloader,
)
