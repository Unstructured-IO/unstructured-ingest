from __future__ import annotations

import datetime
import hashlib
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any, Generator, List, AsyncGenerator

from pydantic import Field, Secret

from unstructured_ingest.error import (
    SourceConnectionError,
)
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
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

from .wrapper import ZendeskClient, ZendeskTicket, ZendeskArticle   

CONNECTOR_TYPE = "zendesk"


class ZendeskAccessConfig(AccessConfig):
    api_token: str = Field(
        description="API token for zendesk generated under Apps and Integrations"
    )


class ZendeskConnectionConfig(ConnectionConfig):
    subdomain: str = Field(description="Subdomain for zendesk site, <sub-domain>.company.com")
    email: str = Field(description="Email for zendesk site registered at the subdomain")
    access_config: Secret[ZendeskAccessConfig]

    @contextmanager
    def get_client(self) -> Generator["ZendeskClient", None, None]:

        access_config = self.access_config.get_secret_value()

        options = {
            "email": self.email,
            "subdomain": self.subdomain,
            "token": access_config.api_token,
        }

        client = ZendeskClient(**options)
        yield client


class ZendeskIndexerConfig(IndexerConfig):
    batch_size: int = Field(
        default=1,
        description="[NotImplemented]Number of tickets: Currently batching is not supported.",
    )
    item_type: str = Field(
        default='tickets',
        description="Type of item from zendesk to parse, can only be tickets or articles."
    )

@dataclass
class ZendeskIndexer(Indexer):
    connection_config: ZendeskConnectionConfig
    index_config: ZendeskIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        """Validates connection to Zendesk API."""
        try:
            with self.connection_config.get_client() as client:
                if not client.get_users():
                    subdomain_endpoint = f"{self.connection_config.subdomain}.zendesk.com"
                    raise SourceConnectionError(
                        f"Users do not exist in subdomain {subdomain_endpoint}"
                    )
        except Exception as e:
            logger.error(f"Failed to validate connection to Zendesk: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")
    
    def is_async(self) -> bool:
        return False

    async def _list_articles_async(self) -> List[ZendeskArticle]:
        async with self.connection_config.get_client() as client:
            return await client.get_articles_async()

    def _list_articles(self) -> List[ZendeskArticle]:
        with self.connection_config.get_client() as client:
            return client.get_articles()

    async def _list_tickets_async(self) -> List[ZendeskTicket]:
        async with self.connection_config.get_client() as client:
            return await client.get_tickets_async()

    def _list_tickets(self) -> List[ZendeskTicket]:
        with self.connection_config.get_client() as client:
            return client.get_tickets()

    def _generate_fullpath(self, identifier: str) -> Path:
        return Path(hashlib.sha256(identifier.encode("utf-8")).hexdigest()[:16] + ".txt")

    def handle_articles(self, articles: List[ZendeskArticle]) -> Generator[FileData, None, None]:
        """Parses articles from a list and yields FileData objects."""
        for article in articles:
            metadata = FileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "id": str(article.id),
                    "author_id": str(article.author_id),
                    "title": str(article.title),
                    "content": str(article.content),
                    "item_type": "article",
                },
            )
            full_path = self._generate_fullpath(str(article.id))
            source_identifiers = SourceIdentifiers(filename=full_path.name, fullpath=str(full_path))

            yield FileData(
                identifier=str(article.id),
                connector_type=self.connector_type,
                metadata=metadata,
                source_identifiers=source_identifiers,
            )

    def handle_tickets(self, tickets: List[ZendeskTicket]) -> Generator[FileData, None, None]:
        """Parses tickets from a list and yields FileData objects."""
        for ticket in tickets:
            metadata = FileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "id": str(ticket.id),
                    "subject": str(ticket.subject),
                    "description": str(ticket.description),
                    "item_type": "ticket",
                },
            )
            full_path = self._generate_fullpath(str(ticket.id))
            source_identifiers = SourceIdentifiers(filename=full_path.name, fullpath=str(full_path))

            yield FileData(
                identifier=str(ticket.id),
                connector_type=self.connector_type,
                metadata=metadata,
                source_identifiers=source_identifiers,
            )

    async def run_async(self, **kwargs: Any) -> AsyncGenerator[FileData, None]:
        """Determines item type and processes accordingly."""
        item_type = self.index_config.item_type

        if item_type == "articles":
            articles = await self._list_articles_async()
            async for file_data in self.handle_articles(articles):
                yield file_data

        elif item_type == "tickets":
            tickets = await self._list_tickets_async()
            async for file_data in self.handle_tickets(tickets):
                yield file_data
    
    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        """Determines item type and processes accordingly."""
        item_type = self.index_config.item_type

        if item_type == "articles":
            articles = self._list_articles()
            yield from self.handle_articles(articles)
        
        elif item_type == "tickets":
            tickets = self._list_tickets()
            yield from self.handle_tickets(tickets)

class ZendeskDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class ZendeskDownloader(Downloader):
    download_config: ZendeskDownloaderConfig
    connection_config: ZendeskConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @SourceConnectionError.wrap
    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:

        zendesk_filedata: FileData = FileData.cast(file_data=file_data)

        with self.connection_config.get_client() as client:

            comments = []
            # each ticket consists of comments, to which I will dump in a txt file.
            first_date = None
            for comment in client.get_comments(ticket_id=zendesk_filedata.identifier):

                if isinstance(comment.metadata["created_at"], datetime.datetime):
                    date_created = comment.metadata["created_at"].isoformat()
                else:
                    date_created = str(comment.metadata["created_at"])

                if first_date is None:
                    first_date = date_created

                comments.append(
                    {
                        "comment_id": comment.id,
                        "author_id": comment.author_id,
                        "body": comment.body,
                        "date_created": date_created,
                    }
                )

            cast_file_data = FileData.cast(file_data=file_data)
            cast_file_data.identifier = file_data.identifier

            # Determine the download path
            download_path = self.get_download_path(file_data=cast_file_data)
            if download_path is None:
                raise ValueError("Download path could not be determined")

            download_path.parent.mkdir(parents=True, exist_ok=True)

            # Write the values to the file
            with open(download_path, "w", encoding="utf8") as f:

                # handle json dump here
                f.write("ticket\n")
                f.write(file_data.identifier)
                f.write("\n")
                f.write(zendesk_filedata.metadata.record_locator["subject"])
                f.write("\n")
                f.write(zendesk_filedata.metadata.record_locator["description"])
                f.write("\n")
                f.write(first_date)
                f.write("\n")
                for comment in comments:
                    f.write("comment")
                    f.write("\n")
                    f.write(str(comment["comment_id"]))
                    f.write("\n")
                    f.write(str(comment["author_id"]))
                    f.write("\n")
                    f.write(comment["body"])
                    f.write("\n")
                    f.write(comment["date_created"])
                    f.write("\n")

            # Update metadata
            cast_file_data.metadata.date_created = first_date

            return super().generate_download_response(
                file_data=cast_file_data, download_path=download_path
            )


# create entry
zendesk_source_entry = SourceRegistryEntry(
    connection_config=ZendeskConnectionConfig,
    indexer_config=ZendeskIndexerConfig,
    indexer=ZendeskIndexer,
    downloader=ZendeskDownloader,
    downloader_config=ZendeskDownloaderConfig,
)
