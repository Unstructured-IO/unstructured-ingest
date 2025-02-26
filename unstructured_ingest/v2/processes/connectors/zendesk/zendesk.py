from __future__ import annotations

import datetime
import hashlib
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any, Generator, List, AsyncGenerator
import aiofiles
import bs4

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

from .wrapper import ZendeskClient, ZendeskTicket, ZendeskArticle, Comment 

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

    @asynccontextmanager
    async def get_client_async(self) -> AsyncGenerator["ZendeskClient", None]:
        """Provides an async context manager for ZendeskClient."""
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
        except SourceConnectionError as e:
            logger.error(f"Source connection error: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Failed to validate connection to Zendesk: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")
    
    def is_async(self) -> bool:
        return True 

    async def _list_articles_async(self) -> List[ZendeskArticle]:
        async with self.connection_config.get_client_async() as client:
            return await client.get_articles_async()

    def _list_articles(self) -> List[ZendeskArticle]:
        with self.connection_config.get_client() as client:
            return client.get_articles()

    async def _list_tickets_async(self) -> List[ZendeskTicket]:
        async with self.connection_config.get_client_async() as client:
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
                    "item_type": "articles",
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

    async def handle_articles_async(self, articles: List[ZendeskArticle]) -> AsyncGenerator[FileData, None]:
        """Parses articles from a list and yields FileData objects asynchronously."""
        for article in articles:
            metadata = FileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "id": str(article.id),
                    "author_id": str(article.author_id),
                    "title": str(article.title),
                    "content": str(article.content),
                    "item_type": "articles",
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
                    "item_type": "tickets",
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

    # Asynchronous version of handle_tickets
    async def handle_tickets_async(self, tickets: List[ZendeskTicket]) -> AsyncGenerator[FileData, None]:
        """Parses tickets from a list and yields FileData objects asynchronously."""
        for ticket in tickets:
            metadata = FileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "id": str(ticket.id),
                    "subject": str(ticket.subject),
                    "description": str(ticket.description),
                    "item_type": "tickets",
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
        """Determines item type and processes accordingly asynchronously."""
        item_type = self.index_config.item_type

        if item_type == "articles":
            articles = await self._list_articles_async()
            async for file_data in self.handle_articles_async(articles):  # Using async version
                yield file_data

        elif item_type == "tickets":
            tickets = await self._list_tickets_async()
            async for file_data in self.handle_tickets_async(tickets):  # Using async version
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

    def handle_articles(self, client, file_data: FileData):
        """
        processes the ticket information, downloads the comments for each ticket
        and proceeds accordingly. 
        """
        file_data: FileData = FileData.cast(file_data=file_data)

        # Determine the download path
        download_path = self.get_download_path(file_data)
        if download_path is None:
            raise ValueError("Download path could not be determined")

        download_path.parent.mkdir(parents=True, exist_ok=True)

        html_data_str = file_data.metadata.record_locator['content']

        soup = bs4.BeautifulSoup(html_data_str, "html.parser")

        # get article attachments
        image_data_decoded: List = client.get_article_attachments(article_id=file_data.metadata.record_locator['id'])
        img_tags = soup.find_all("img")

        for i, img_tag in enumerate(img_tags):
            img_tag['src'] = image_data_decoded[i]['encoded_content']
        
        file_data.metadata.record_locator['content'] = str(soup)

        # Write the values to the file
        with open(download_path, "w", encoding="utf8") as f:
            f.write("article\n")
            f.write(file_data.identifier + "\n")
            f.write(file_data.metadata.record_locator["title"] + "\n")
            f.write(file_data.metadata.record_locator['content'] + "\n")
            f.write(file_data.metadata.record_locator['author_id'] + "\n")

        return super().generate_download_response(
            file_data=file_data, download_path=download_path
        )

    def handle_tickets(self, client, file_data: FileData) -> DownloadResponse:
        """
        processes an article's information, parses it and proceeds accordingly. 
        """

        comments: List[Comment] = [] 

        # each ticket consists of comments, to which I will dump in a txt file.
        first_date = None
        for comment in client.get_comments(ticket_id=file_data.identifier):

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

        # Determine the download path
        download_path = self.get_download_path(file_data)
        if download_path is None:
            raise ValueError("Download path could not be determined")

        download_path.parent.mkdir(parents=True, exist_ok=True)

        # Write the values to the file
        with open(download_path, "w", encoding="utf8") as f:
            f.write("ticket\n")
            f.write(file_data.identifier + "\n")
            f.write(file_data.metadata.record_locator["subject"] + "\n")
            f.write(file_data.metadata.record_locator["description"] + "\n")
            f.write(first_date + "\n")
            for comment in comments:
                f.write("comment\n")
                f.write(str(comment["comment_id"]) + "\n")
                f.write(str(comment["author_id"]) + "\n")
                f.write(comment["body"] + "\n")
                f.write(comment["date_created"] + "\n")

        return super().generate_download_response(
            file_data=file_data, download_path=download_path
        )

    async def handle_articles_async(self, client: ZendeskClient, file_data: FileData):
        """
        Processes the ticket information, downloads the comments for each ticket, 
        and proceeds accordingly.
        """
        file_data: FileData = FileData.cast(file_data=file_data)

        # Determine the download path
        download_path = self.get_download_path(file_data)
        if download_path is None:
            raise ValueError("Download path could not be determined")

        download_path.parent.mkdir(parents=True, exist_ok=True)

        html_data_str = file_data.metadata.record_locator['content']
        soup = bs4.BeautifulSoup(html_data_str, "html.parser")

        # Get article attachments asynchronously
        article_id = file_data.metadata.record_locator.get('id')
        if article_id is None:
            raise ValueError("Article ID is missing in metadata")

        image_data_decoded: List = await client.get_article_attachments_async(article_id=article_id)
        img_tags = soup.find_all("img")

        # Ensure we don't exceed the available images
        for img_tag, img_data in zip(img_tags, image_data_decoded):
            img_tag['src'] = img_data.get('encoded_content', '')

        # Update content with modified images
        file_data.metadata.record_locator['content'] = str(soup)

        # Asynchronously write the values to the file
        async with aiofiles.open(download_path, "w", encoding="utf8") as f:
            await f.write("article\n")
            await f.write(f"{file_data.identifier}\n")
            await f.write(f"{file_data.metadata.record_locator.get('title', '')}\n")
            await f.write(f"{file_data.metadata.record_locator['content']}\n")
            await f.write(f"{file_data.metadata.record_locator.get('author_id', '')}\n")

        return super().generate_download_response(
            file_data=file_data, download_path=download_path
        )


    async def handle_tickets_async(self, client: ZendeskClient, file_data: FileData) -> DownloadResponse:
        """
        processes an article's information, parses it and proceeds accordingly. 
        """

        comments: List[Comment] = [] 

        # each ticket consists of comments, to which I will dump in a txt file.
        first_date = None
        for comment in await client.get_comments_async(ticket_id=file_data.identifier):

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

        # Determine the download path
        download_path = self.get_download_path(file_data)
        if download_path is None:
            raise ValueError("Download path could not be determined")

        download_path.parent.mkdir(parents=True, exist_ok=True)

        # Asynchronously write the values to the file
        async with aiofiles.open(download_path, "w", encoding="utf8") as f:
            await f.write("ticket\n")
            await f.write(file_data.identifier + "\n")
            await f.write(file_data.metadata.record_locator["subject"] + "\n")
            await f.write(file_data.metadata.record_locator["description"] + "\n")
            await f.write(first_date + "\n")
            for comment in comments:
                await f.write("comment\n")
                await f.write(str(comment["comment_id"]) + "\n")
                await f.write(str(comment["author_id"]) + "\n")
                await f.write(comment["body"] + "\n")
                await f.write(comment["date_created"] + "\n")

        return super().generate_download_response(
            file_data=file_data, download_path=download_path
        )

    @SourceConnectionError.wrap
    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:

        zendesk_filedata: FileData = FileData.cast(file_data=file_data)

        async with self.connection_config.get_client_async() as client:
            
            item_type = zendesk_filedata.metadata.record_locator['item_type']
            cast_file_data = FileData.cast(file_data=file_data)
            cast_file_data.identifier = file_data.identifier

            if item_type == "articles":
                return await self.handle_articles_async(client, cast_file_data)
            elif item_type == "tickets":
                return await self.handle_tickets_async(client, cast_file_data)
            else: 
                raise RuntimeError(f"Item type {item_type} cannot be handled by the downloader")

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        zendesk_filedata: FileData = FileData.cast(file_data=file_data)

        with self.connection_config.get_client() as client:
            
            item_type = zendesk_filedata.metadata.record_locator['item_type']
            cast_file_data = FileData.cast(file_data=file_data)
            cast_file_data.identifier = file_data.identifier

            if item_type == "articles":
                return self.handle_articles(client, cast_file_data)
            elif item_type == "tickets":
                return self.handle_tickets(client, cast_file_data)
            else: 
                raise RuntimeError(f"Item type {item_type} cannot be handled by the downloader")

# create entry
zendesk_source_entry = SourceRegistryEntry(
    connection_config=ZendeskConnectionConfig,
    indexer_config=ZendeskIndexerConfig,
    indexer=ZendeskIndexer,
    downloader=ZendeskDownloader,
    downloader_config=ZendeskDownloaderConfig,
)
