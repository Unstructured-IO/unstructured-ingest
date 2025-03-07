from __future__ import annotations

import datetime
import hashlib
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any, AsyncGenerator, List, Literal

from pydantic import BaseModel, Field, Secret

from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.html import HtmlMixin
from unstructured_ingest.v2.errors import UserAuthError
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

    async def get_client_async(self) -> ZendeskClient:
        """Provides an async manager for ZendeskClient."""
        access_config = self.access_config.get_secret_value()

        client = ZendeskClient(
            email=self.email, subdomain=self.subdomain, token=access_config.api_token
        )
        return client

    def get_client(self) -> ZendeskClient:

        access_config = self.access_config.get_secret_value()

        client = ZendeskClient(
            email=self.email, subdomain=self.subdomain, token=access_config.api_token
        )
        return client


class ZendeskIndexerConfig(IndexerConfig):
    batch_size: int = Field(
        default=2,
        description="Number of tickets or articles.",
    )
    item_type: Literal["tickets", "articles", "all"] = Field(
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
            client = self.connection_config.get_client()
            if not client.get_users():
                subdomain_endpoint = f"{self.connection_config.subdomain}.zendesk.com"
                raise UserAuthError(f"Users do not exist in subdomain {subdomain_endpoint}")
        except UserAuthError as e:
            logger.error(f"Source connection error: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Failed to validate connection to Zendesk: {e}", exc_info=True)
            raise UserAuthError(f"Failed to validate connection: {e}")

    def is_async(self) -> bool:
        return True

    async def _list_articles_async(self) -> List[ZendeskArticle]:
        client = await self.connection_config.get_client_async()
        return await client.get_articles_async()

    async def _list_tickets_async(self) -> List[ZendeskTicket]:
        client = await self.connection_config.get_client_async()
        return await client.get_tickets_async()

    def _generate_fullpath(self, identifier: str) -> Path:
        return Path(hashlib.sha256(identifier.encode("utf-8")).hexdigest()[:16] + ".txt")

    async def handle_articles_async(
        self, articles: List[ZendeskArticle], batch_size: int
    ) -> AsyncGenerator[ZendeskBatchFileData, None]:
        """Parses articles from a list and yields FileData objects asynchronously in batches."""
        for article_batch in batch_generator(articles, batch_size=batch_size):

            article_batch = sorted(article_batch)

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
            full_path = Path(str(full_path).replace(".txt", ".html"))

            source_identifiers = SourceIdentifiers(filename=full_path.name, fullpath=str(full_path))

            batched_file_data = ZendeskBatchFileData(
                identifier=str(article_batch[0].id),
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

            sorted_batch = sorted(ticket_batch)

            additional_metadata = ZendeskAdditionalMetadata(
                item_type="tickets",
                leading_id=str(sorted_batch[0].id),
                tail_id=str(sorted_batch[-1].id),
            )

            metadata = ZendeskFileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "id": str(sorted_batch[0].id),
                    "item_type": "tickets",
                },
            )

            batch_items: List[ZendeskBatchItemTicket] = [
                ZendeskBatchItemTicket(
                    identifier=str(ticket.id),
                    subject=str(ticket.subject),
                    description=str(ticket.description),
                )
                for ticket in sorted_batch
            ]

            full_path = self._generate_fullpath(str(sorted_batch[0].id))
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


class ZendeskDownloaderConfig(DownloaderConfig, HtmlMixin):
    pass


@dataclass
class ZendeskDownloader(Downloader):
    download_config: ZendeskDownloaderConfig
    connection_config: ZendeskConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        return True

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

    @requires_dependencies(["bs4", "aiofiles"], extras="zendesk")
    async def handle_articles_async(
        self, client: ZendeskClient, batch_file_data: ZendeskBatchFileData
    ):
        """
        Processes the article information, downloads the attachments for each article,
        and updates the content accordingly.
        """
        import aiofiles
        import bs4

        # Determine the download path
        download_path = self.get_download_path(batch_file_data)

        if download_path is None:
            raise ValueError("Download path could not be determined")

        download_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(download_path, "a", encoding="utf8") as f:
            for article in batch_file_data.batch_items:
                html_data_str = article.content
                soup = bs4.BeautifulSoup(html_data_str, "html.parser")

                if self.download_config.extract_images:
                    # Get article attachments asynchronously
                    image_data_decoded: List = await client.get_article_attachments_async(
                        article_id=article.identifier
                    )
                    img_tags = soup.find_all("img")

                    # Ensure we don't exceed the available images
                    for img_tag, img_data in zip(img_tags, image_data_decoded):
                        img_tag["src"] = img_data.get("encoded_content", "")

            await f.write(soup.prettify())

        return super().generate_download_response(
            file_data=batch_file_data, download_path=download_path
        )

    @requires_dependencies(["aiofiles"], extras="zendesk")
    async def handle_tickets_async(
        self, client: ZendeskClient, batch_file_data: ZendeskBatchFileData
    ) -> DownloadResponse:
        """
        Processes a batch of tickets asynchronously, writing their details and comments to a file.
        """
        import aiofiles

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
                comments_list = await client.get_comments_async(ticket_id=int(ticket_identifier))

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

    async def run_async(self, file_data: ZendeskBatchFileData, **kwargs: Any) -> DownloadResponse:

        zendesk_filedata: FileData = FileData.cast(file_data=file_data)

        client = await self.connection_config.get_client_async()
        item_type = zendesk_filedata.metadata.record_locator["item_type"]

        if item_type == "articles":
            return await self.handle_articles_async(client, file_data)
        elif item_type == "tickets":
            return await self.handle_tickets_async(client, file_data)
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
