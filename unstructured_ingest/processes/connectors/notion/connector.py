from dataclasses import dataclass
from time import time
from typing import TYPE_CHECKING, Any, AsyncGenerator, Generator, Optional

from pydantic import UUID4, Field, Secret

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
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import SourceRegistryEntry
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from unstructured_ingest.processes.connectors.notion.client import Client

NOTION_API_VERSION = "2022-06-28"
CONNECTOR_TYPE = "notion"


class NotionAccessConfig(AccessConfig):
    notion_api_key: str = Field(description="Notion API key")


class NotionConnectionConfig(ConnectionConfig):
    access_config: Secret[NotionAccessConfig]

    @requires_dependencies(["notion_client"], extras="notion")
    def get_client(self) -> "Client":
        from unstructured_ingest.processes.connectors.notion.client import Client

        return Client(
            notion_version=NOTION_API_VERSION,
            auth=self.access_config.get_secret_value().notion_api_key,
            logger=logger,
            log_level=logger.level,
        )


class NotionIndexerConfig(IndexerConfig):
    page_ids: Optional[list[str]] = Field(
        default=None, description="List of Notion page IDs to process"
    )

    database_ids: Optional[list[str]] = Field(
        default=None, description="List of Notion database IDs to process"
    )
    recursive: bool = Field(
        default=False, description="Recursively process child pages and databases"
    )

    def __post_init__(self):
        if self.page_ids:
            self.page_ids: list[UUID4] = [UUID4(p.strip()) for p in self.page_ids]

        if self.database_ids:
            self.database_ids: list[UUID4] = [UUID4(p.strip()) for p in self.database_ids]


@dataclass
class NotionIndexer(Indexer):
    connection_config: NotionConnectionConfig
    index_config: NotionIndexerConfig

    def is_async(self) -> bool:
        return False

    def precheck(self) -> None:
        """Check the connection to the Notion API."""
        try:
            client = self.connection_config.get_client()
            # Perform a simple request to verify connection
            request = client._build_request("HEAD", "users")
            response = client.client.send(request)
            response.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        client = self.connection_config.get_client()
        processed_pages: set[str] = set()
        processed_databases: set[str] = set()

        pages_to_process: set[str] = set(self.index_config.page_ids or [])
        databases_to_process: set[str] = set(self.index_config.database_ids or [])

        while pages_to_process or databases_to_process:
            # Process pages
            for page_id in list(pages_to_process):
                if page_id in processed_pages:
                    continue

                processed_pages.add(page_id)
                pages_to_process.remove(page_id)
                file_data = self.get_page_file_data(page_id=page_id, client=client)
                if file_data:
                    yield file_data

                if self.index_config.recursive:
                    (child_pages, child_databases) = self.get_child_pages_and_databases(
                        page_id=page_id,
                        client=client,
                        processed_pages=processed_pages,
                        processed_databases=processed_databases,
                    )
                    pages_to_process.update(child_pages)
                    databases_to_process.update(child_databases)

            # Process databases
            for database_id in list(databases_to_process):
                if database_id in processed_databases:
                    continue
                processed_databases.add(database_id)
                databases_to_process.remove(database_id)
                file_data = self.get_database_file_data(database_id=database_id, client=client)
                if file_data:
                    yield file_data
                if self.index_config.recursive:
                    (
                        child_pages,
                        child_databases,
                    ) = self.get_child_pages_and_databases_from_database(
                        database_id=database_id,
                        client=client,
                        processed_pages=processed_pages,
                        processed_databases=processed_databases,
                    )
                    pages_to_process.update(child_pages)
                    databases_to_process.update(child_databases)

    @requires_dependencies(["notion_client"], extras="notion")
    def get_page_file_data(self, page_id: str, client: "Client") -> Optional[FileData]:
        try:
            page_metadata = client.pages.retrieve(page_id=page_id)  # type: ignore
            date_created = page_metadata.created_time
            date_modified = page_metadata.last_edited_time
            identifier = page_id
            source_identifiers = SourceIdentifiers(
                filename=f"{page_id}.html",
                fullpath=f"{page_id}.html",
                rel_path=f"{page_id}.html",
            )
            metadata = FileDataSourceMetadata(
                date_created=date_created,
                date_modified=date_modified,
                record_locator={"page_id": page_id},
                date_processed=str(time()),
            )
            # additional_metadata = page_metadata
            additional_metadata = {
                "created_by": page_metadata.created_by,
                "last_edited_by": page_metadata.last_edited_by,
                "parent": page_metadata.parent,
                "url": page_metadata.url,
            }

            return FileData(
                identifier=identifier,
                connector_type=CONNECTOR_TYPE,
                source_identifiers=source_identifiers,
                metadata=metadata,
                additional_metadata=additional_metadata,
                display_name=source_identifiers.fullpath,
            )
        except Exception as e:
            logger.error(f"Error retrieving page {page_id}: {e}")
            return None

    @requires_dependencies(["notion_client"], extras="notion")
    def get_database_file_data(self, database_id: str, client: "Client") -> Optional[FileData]:
        try:
            # type: ignore
            database_metadata = client.databases.retrieve(database_id=database_id)
            date_created = database_metadata.created_time
            date_modified = database_metadata.last_edited_time
            identifier = database_id
            source_identifiers = SourceIdentifiers(
                filename=f"{database_id}.html",
                fullpath=f"{database_id}.html",
                rel_path=f"{database_id}.html",
            )
            metadata = FileDataSourceMetadata(
                date_created=date_created,
                date_modified=date_modified,
                record_locator={"database_id": database_id},
                date_processed=str(time()),
            )
            additional_metadata = {
                "created_by": database_metadata.created_by,
                "last_edited_by": database_metadata.last_edited_by,
                "parent": database_metadata.parent,
                "url": database_metadata.url,
            }
            return FileData(
                identifier=identifier,
                connector_type=CONNECTOR_TYPE,
                source_identifiers=source_identifiers,
                metadata=metadata,
                additional_metadata=additional_metadata,
                display_name=source_identifiers.fullpath,
            )
        except Exception as e:
            logger.error(f"Error retrieving database {database_id}: {e}")
            return None

    def get_child_pages_and_databases(
        self,
        page_id: str,
        client: "Client",
        processed_pages: set[str],
        processed_databases: set[str],
    ) -> tuple[set[str], set[str]]:
        from unstructured_ingest.processes.connectors.notion.helpers import (
            get_recursive_content_from_page,
        )

        child_content = get_recursive_content_from_page(
            client=client,
            page_id=page_id,
            logger=logger,
        )
        child_pages = set(child_content.child_pages) - processed_pages
        child_databases = set(child_content.child_databases) - processed_databases
        return child_pages, child_databases

    def get_child_pages_and_databases_from_database(
        self,
        database_id: str,
        client: "Client",
        processed_pages: set[str],
        processed_databases: set[str],
    ) -> tuple[set[str], set[str]]:
        from unstructured_ingest.processes.connectors.notion.helpers import (
            get_recursive_content_from_database,
        )

        child_content = get_recursive_content_from_database(
            client=client,
            database_id=database_id,
            logger=logger,
        )
        child_pages = set(child_content.child_pages) - processed_pages
        child_databases = set(child_content.child_databases) - processed_databases
        return child_pages, child_databases

    async def run_async(self, **kwargs: Any) -> AsyncGenerator[None, None]:
        # Asynchronous run is not implemented
        raise NotImplementedError()


class NotionDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class NotionDownloader(Downloader):
    connection_config: NotionConnectionConfig
    download_config: NotionDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        client = self.connection_config.get_client()
        record_locator = file_data.metadata.record_locator

        if "page_id" in record_locator:
            return self.download_page(
                client=client,
                page_id=record_locator["page_id"],
                file_data=file_data,
            )
        elif "database_id" in record_locator:
            return self.download_database(
                client=client,
                database_id=record_locator["database_id"],
                file_data=file_data,
            )
        else:
            raise ValueError("Invalid record_locator in file_data")

    def download_page(self, client, page_id: str, file_data: FileData) -> DownloadResponse:
        from unstructured_ingest.processes.connectors.notion.helpers import extract_page_html

        try:
            text_extraction = extract_page_html(
                client=client,
                page_id=page_id,
                logger=logger,
            )

            if text_extraction.html:
                download_path = self.get_download_path(file_data=file_data)
                download_path.parent.mkdir(parents=True, exist_ok=True)
                with download_path.open("w") as page_file:
                    page_file.write(text_extraction.html.render(pretty=True))
                return self.generate_download_response(
                    file_data=file_data, download_path=download_path
                )
            else:
                logger.error(f"No HTML content for page {page_id}")
                return None
        except Exception as e:
            logger.error(f"Error downloading page {page_id}: {e}")
            return None

    def download_database(self, client, database_id: str, file_data: FileData) -> DownloadResponse:
        from unstructured_ingest.processes.connectors.notion.helpers import extract_database_html

        try:
            text_extraction = extract_database_html(
                client=client,
                database_id=database_id,
                logger=logger,
            )
            if text_extraction.html:
                download_path = self.get_download_path(file_data=file_data)
                download_path.parent.mkdir(parents=True, exist_ok=True)
                with download_path.open("w") as database_file:
                    database_file.write(text_extraction.html.render(pretty=True))
                return self.generate_download_response(
                    file_data=file_data, download_path=download_path
                )
            else:
                logger.error(f"No HTML content for database {database_id}")
                return None
        except Exception as e:
            logger.error(f"Error downloading database {database_id}: {e}")
            return None


notion_source_entry = SourceRegistryEntry(
    connection_config=NotionConnectionConfig,
    indexer_config=NotionIndexerConfig,
    indexer=NotionIndexer,
    downloader_config=NotionDownloaderConfig,
    downloader=NotionDownloader,
)
