from __future__ import annotations

import datetime
import hashlib
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any, AsyncGenerator, Generator, List

import aiofiles
import bs4
from pydantic import BaseModel, Field, Secret

from unstructured_ingest.error import (
    SourceConnectionError,
)
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    BatchFileData,
    BatchItem,
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

from .client import ZendeskArticle, ZendeskClient, ZendeskTicket

CONNECTOR_TYPE = "zendesk"


class ZendeskAdditionalMetadata(BaseModel):
    item_type: str
    leading_id: str  # is the same as id just being verbose.
    tail_id: str  # last id in the batch.


class ZendeskFileDataSourceMetadata(FileDataSourceMetadata):
    """
    inherits metadata object as tickets and articles
    are treated in single batch, we need to denote indices ticket/article
    as the source metadata.
    """


class ZendeskBatchFileData(BatchFileData):
    additional_metadata: ZendeskAdditionalMetadata


class ZendeskAccessConfig(AccessConfig):
    api_token: str = Field(
        description="API token for zendesk generated under Apps and Integrations"
    )


class ZendeskBatchItemTicket(BatchItem):
    subject: str
    description: str
    item_type: str = "tickets"  # placeholder for downloader


class ZendeskBatchItemArticle(BatchItem):
    title: str
    author_id: str
    title: str
    content: str


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
        description="Number of tickets or articles.",
    )
    item_type: str = Field(
        default="tickets",
        description="Type of item from zendesk to parse, can only be `tickets` or `articles`.",
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
        return True  # TODO(set to true when testing async and before PR.)

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

    def handle_articles(
        self, articles: List[ZendeskArticle], batch_size: int
    ) -> Generator[ZendeskBatchFileData, None, None]:
        """Parses articles from a list and yields FileData objects."""

        for article_batch in batch_generator(articles, batch_size=batch_size):
            additional_metadata = ZendeskAdditionalMetadata(
                item_type="articles",
                leading_id=str(article_batch[0].id),
                tail_id=str(article_batch[-1].id),
            )

            metadata = ZendeskFileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "id": str(article_batch[0].id),
                    "item_type": "articles",
                },
            )

            batch_items: List[ZendeskBatchItemArticle] = [
                ZendeskBatchItemArticle(
                    identifier=str(article.id),
                    author_id=str(article.author_id),
                    title=str(article.title),
                    content=str(article.content),
                )
                for article in article_batch
            ]

            full_path = self._generate_fullpath(str(article_batch[0].id))
            source_identifiers = SourceIdentifiers(filename=full_path.name, fullpath=str(full_path))

            batched_file_data = ZendeskBatchFileData(
                connector_type=self.connector_type,
                metadata=metadata,
                batch_items=batch_items,
                additional_metadata=additional_metadata,
                source_identifiers=source_identifiers,
            )

            yield batched_file_data

    async def handle_articles_async(
        self, articles: List[ZendeskArticle], batch_size: int
    ) -> AsyncGenerator[ZendeskBatchFileData, None]:
        """Parses articles from a list and yields FileData objects asynchronously in batches."""
        for article_batch in batch_generator(articles, batch_size=batch_size):
            additional_metadata = ZendeskAdditionalMetadata(
                item_type="articles",
                leading_id=str(article_batch[0].id),
                tail_id=str(article_batch[-1].id),
            )

            metadata = ZendeskFileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "id": str(article_batch[0].id),
                    "item_type": "articles",
                },
            )

            batch_items: List[ZendeskBatchItemArticle] = [
                ZendeskBatchItemArticle(
                    identifier=str(article.id),
                    author_id=str(article.author_id),
                    title=str(article.title),
                    content=str(article.content),
                )
                for article in article_batch
            ]

            full_path = self._generate_fullpath(str(article_batch[0].id))
            source_identifiers = SourceIdentifiers(filename=full_path.name, fullpath=str(full_path))

            batched_file_data = ZendeskBatchFileData(
                connector_type=self.connector_type,
                metadata=metadata,
                batch_items=batch_items,
                additional_metadata=additional_metadata,
                source_identifiers=source_identifiers,
            )

            yield batched_file_data

    def handle_tickets(
        self, tickets: List[ZendeskTicket], batch_size: int
    ) -> Generator[ZendeskBatchFileData, None, None]:
        """Parses tickets from a list and yields FileData objects."""

        for ticket_batch in batch_generator(tickets, batch_size=batch_size):

            additional_metadata = ZendeskAdditionalMetadata(
                leading_id=str(ticket_batch[0].id),
                tail_id=str(ticket_batch[-1].id),
                item_type="tickets",
            )

            metadata = ZendeskFileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "id": str(ticket_batch[0].id),
                    "item_type": "tickets",
                },
            )

            batch_items: List[ZendeskBatchItemTicket] = [
                ZendeskBatchItemTicket(
                    identifier=str(ticket.id),
                    subject=str(ticket.subject),
                    description=str(ticket.description),
                )
                for ticket in ticket_batch
            ]

            # handle identifiers using leading batch id
            full_path = self._generate_fullpath(str(ticket_batch[0].id))
            source_identifiers = SourceIdentifiers(filename=full_path.name, fullpath=str(full_path))

            batched_file_data = ZendeskBatchFileData(
                connector_type=self.connector_type,
                metadata=metadata,
                batch_items=batch_items,
                additional_metadata=additional_metadata,
                source_identifiers=source_identifiers,
            )

            yield batched_file_data

    async def handle_tickets_async(
        self, tickets: List[ZendeskTicket], batch_size: int
    ) -> AsyncGenerator[ZendeskBatchFileData, None]:
        """Parses tickets from a list and yields FileData objects asynchronously in batches."""
        for ticket_batch in batch_generator(tickets, batch_size=batch_size):
            additional_metadata = ZendeskAdditionalMetadata(
                item_type="tickets",
                leading_id=str(ticket_batch[0].id),
                tail_id=str(ticket_batch[-1].id),
            )

            metadata = ZendeskFileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "id": str(ticket_batch[0].id),
                    "item_type": "tickets",
                },
            )

            batch_items: List[ZendeskBatchItemTicket] = [
                ZendeskBatchItemTicket(
                    identifier=str(ticket.id),
                    subject=str(ticket.subject),
                    description=str(ticket.description),
                )
                for ticket in ticket_batch
            ]

            full_path = self._generate_fullpath(str(ticket_batch[0].id))
            source_identifiers = SourceIdentifiers(filename=full_path.name, fullpath=str(full_path))

            batched_file_data = ZendeskBatchFileData(
                connector_type=self.connector_type,
                metadata=metadata,
                batch_items=batch_items,
                additional_metadata=additional_metadata,
                source_identifiers=source_identifiers,
            )

            yield batched_file_data

    async def run_async(self, **kwargs: Any) -> AsyncGenerator[FileData, None]:
        """Determines item type and processes accordingly asynchronously."""
        item_type = self.index_config.item_type
        batch_size = self.index_config.batch_size

        if item_type == "articles":
            articles = await self._list_articles_async()
            async for file_data in self.handle_articles_async(
                articles, batch_size
            ):  # Using async version
                yield file_data

        elif item_type == "tickets":
            tickets = await self._list_tickets_async()
            async for file_data in self.handle_tickets_async(
                tickets, batch_size
            ):  # Using async version
                yield file_data

    def run(self, **kwargs: Any) -> Generator[BatchFileData, None, None]:
        """Determines item type and processes accordingly."""
        item_type = self.index_config.item_type
        batch_size = self.index_config.batch_size

        if item_type == "articles":
            articles = self._list_articles()
            yield from self.handle_articles(articles, batch_size)

        elif item_type == "tickets":
            tickets = self._list_tickets()
            yield from self.handle_tickets(tickets, batch_size)


class ZendeskDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class ZendeskDownloader(Downloader):
    download_config: ZendeskDownloaderConfig
    connection_config: ZendeskConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def handle_articles(self, client, batch_file_data: ZendeskBatchFileData):
        """
        processes the ticket information, downloads the comments for each ticket
        and proceeds accordingly.
        """

        # Determine the download path
        download_path = self.get_download_path(batch_file_data)
        if download_path is None:
            raise ValueError("Download path could not be determined")

        download_path.parent.mkdir(parents=True, exist_ok=True)

        for article in batch_file_data.batch_items:

            html_data_str = article.content
            soup = bs4.BeautifulSoup(html_data_str, "html.parser")

            # get article attachments
            image_data_decoded: List = client.get_article_attachments(article_id=article.identifier)
            img_tags = soup.find_all("img")

            for i, img_tag in enumerate(img_tags):
                img_tag["src"] = image_data_decoded[i]["encoded_content"]

            article.content = str(soup)

            # Write the values to the file
            with open(download_path, "a", encoding="utf8") as f:
                content = (
                    "article\n"
                    f"{article.identifier}\n"
                    f"{article.title}\n"
                    f"{article.content}\n"
                    f"{article.author_id}\n"
                )
                f.write(content)

        return super().generate_download_response(
            file_data=batch_file_data, download_path=download_path
        )

    def handle_tickets(
        self, client: ZendeskClient, batch_file_data: ZendeskBatchFileData
    ) -> DownloadResponse:
        """
        processes an article's information, parses it and proceeds accordingly.
        """

        # Determine the download path
        download_path = self.get_download_path(batch_file_data)
        if download_path is None:
            raise ValueError("Download path could not be determined")

        download_path.parent.mkdir(parents=True, exist_ok=True)

        # go through batches of tickets
        for batch_item in batch_file_data.batch_items:

            # process ith ticket and dump it into file.
            ticket_identifier = batch_item.identifier
            first_date = None
            with open(download_path, "a", encoding="utf8") as f:
                content = (
                    "\nticket\n"
                    f"{batch_item.identifier}\n"
                    f"{batch_file_data.metadata.record_locator.get('subject', '')}\n"
                    f"{batch_file_data.metadata.record_locator.get('description', '')}\n"
                    f"{first_date}\n"
                )

            comments: List[dict] = []

            for comment in client.get_comments(ticket_id=ticket_identifier):

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

            with open(download_path, "a", encoding="utf8") as f:

                for comment in comments:
                    content += (
                        "comment\n"
                        f"{comment.get('comment_id', '')}\n"
                        f"{comment.get('author_id', '')}\n"
                        f"{comment.get('body', '')}\n"
                        f"{comment.get('date_created', '')}\n"
                    )

                f.write(content)

        return super().generate_download_response(
            file_data=batch_file_data, download_path=download_path
        )

    async def handle_articles_async(
        self, client: ZendeskClient, batch_file_data: ZendeskBatchFileData
    ):
        """
        Processes the article information, downloads the attachments for each article,
        and updates the content accordingly.
        """
        # Determine the download path
        download_path = self.get_download_path(batch_file_data)
        if download_path is None:
            raise ValueError("Download path could not be determined")

        download_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(download_path, "a", encoding="utf8") as f:
            for article in batch_file_data.batch_items:
                html_data_str = article.content
                soup = bs4.BeautifulSoup(html_data_str, "html.parser")

                # Get article attachments asynchronously
                image_data_decoded: List = await client.get_article_attachments_async(
                    article_id=article.identifier
                )
                img_tags = soup.find_all("img")

                # Ensure we don't exceed the available images
                for img_tag, img_data in zip(img_tags, image_data_decoded):
                    img_tag["src"] = img_data.get("encoded_content", "")

                # Update content with modified images
                article.content = str(soup)

                # Write the values to the file
                content = (
                    "article\n"
                    f"{article.identifier}\n"
                    f"{article.title}\n"
                    f"{article.content}\n"
                    f"{article.author_id}\n"
                )
                await f.write(content)

        return super().generate_download_response(
            file_data=batch_file_data, download_path=download_path
        )

    async def handle_tickets_async(
        self, client: ZendeskClient, batch_file_data: ZendeskBatchFileData
    ) -> DownloadResponse:
        """
        Processes a batch of tickets asynchronously, writing their details and comments to a file.
        """
        # Determine the download path
        download_path = self.get_download_path(batch_file_data)
        if download_path is None:
            raise ValueError("Download path could not be determined")

        download_path.parent.mkdir(parents=True, exist_ok=True)

        # Process each ticket in the batch
        async with aiofiles.open(download_path, "a", encoding="utf8") as f:
            for batch_item in batch_file_data.batch_items:
                ticket_identifier = batch_item.identifier
                first_date = None
                comments: List[dict] = []

                # Fetch comments asynchronously
                comments_list = await client.get_comments_async(
                    ticket_id=ticket_identifier
                )  # Await the coroutine

                for comment in comments_list:  # Iterate over the resolved list
                    date_created = (
                        comment.metadata["created_at"].isoformat()
                        if isinstance(comment.metadata["created_at"], datetime.datetime)
                        else str(comment.metadata["created_at"])
                    )

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

                # Write ticket details to file
                content = (
                    "\nticket\n"
                    f"{batch_item.identifier}\n"
                    f"{batch_file_data.metadata.record_locator.get('subject', '')}\n"
                    f"{batch_file_data.metadata.record_locator.get('description', '')}\n"
                    f"{first_date}\n"
                )

                # Append comments
                for comment in comments:
                    content += (
                        "comment\n"
                        f"{comment.get('comment_id', '')}\n"
                        f"{comment.get('author_id', '')}\n"
                        f"{comment.get('body', '')}\n"
                        f"{comment.get('date_created', '')}\n"
                    )

                await f.write(content)

        return super().generate_download_response(
            file_data=batch_file_data, download_path=download_path
        )

    @SourceConnectionError.wrap
    async def run_async(self, file_data: ZendeskBatchFileData, **kwargs: Any) -> DownloadResponse:

        zendesk_filedata: FileData = FileData.cast(file_data=file_data)

        async with self.connection_config.get_client_async() as client:

            item_type = zendesk_filedata.metadata.record_locator["item_type"]

            if item_type == "articles":
                return await self.handle_articles_async(client, file_data)
            elif item_type == "tickets":
                return await self.handle_tickets_async(client, file_data)
            else:
                raise RuntimeError(f"Item type {item_type} cannot be handled by the downloader")

    def run(self, file_data: ZendeskBatchFileData, **kwargs: Any) -> DownloadResponse:

        with self.connection_config.get_client() as client:

            item_type = file_data.metadata.record_locator["item_type"]

            if item_type == "articles":
                return self.handle_articles(client, file_data)
            elif item_type == "tickets":
                return self.handle_tickets(client, file_data)
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
