from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any, AsyncGenerator, Literal, Union

from pydantic import BaseModel, Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
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
from unstructured_ingest.utils.html import HtmlMixin

from .client import ZendeskArticle, ZendeskClient, ZendeskTicket

CONNECTOR_TYPE = "zendesk"


class ZendeskAdditionalMetadata(BaseModel):
    item_type: Literal["ticket", "article"]
    content: Union[ZendeskTicket, ZendeskArticle]


class ZendeskFileData(FileData):
    additional_metadata: ZendeskAdditionalMetadata


class ZendeskAccessConfig(AccessConfig):
    api_token: str = Field(
        description="API token for zendesk generated under Apps and Integrations"
    )


class ZendeskConnectionConfig(ConnectionConfig):
    subdomain: str = Field(description="Subdomain for zendesk site, <sub-domain>.company.com")
    email: str = Field(description="Email for zendesk site registered at the subdomain")
    access_config: Secret[ZendeskAccessConfig]

    def get_client(self) -> ZendeskClient:
        access_config = self.access_config.get_secret_value()

        return ZendeskClient(
            email=self.email, subdomain=self.subdomain, token=access_config.api_token
        )


class ZendeskIndexerConfig(IndexerConfig):
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
        self.connection_config.get_client()

    def is_async(self) -> bool:
        return True

    def _generate_fullpath(self, identifier: str) -> Path:
        return Path(hashlib.sha256(identifier.encode("utf-8")).hexdigest()[:16] + ".txt")

    async def get_tickets(self) -> AsyncGenerator[ZendeskFileData, None]:
        async with self.connection_config.get_client() as client:
            async for ticket in client.get_tickets():
                source_identifiers = SourceIdentifiers(
                    filename=f"{ticket.id}.txt", fullpath=f"tickets/{ticket.id}.txt"
                )
                yield ZendeskFileData(
                    identifier=str(ticket.id),
                    connector_type=self.connector_type,
                    source_identifiers=source_identifiers,
                    additional_metadata=ZendeskAdditionalMetadata(
                        item_type="ticket", content=ticket
                    ),
                    metadata=FileDataSourceMetadata(
                        url=str(ticket.url) if ticket.url else None,
                        date_created=ticket.created_at.isoformat() if ticket.created_at else None,
                        date_modified=ticket.updated_at.isoformat() if ticket.updated_at else None,
                        date_processed=str(time()),
                    ),
                    display_name=source_identifiers.fullpath,
                )

    async def get_articles(self) -> AsyncGenerator[ZendeskFileData, None]:
        async with self.connection_config.get_client() as client:
            async for article in client.get_articles():
                source_identifiers = SourceIdentifiers(
                    filename=f"{article.id}.html", fullpath=f"articles/{article.id}.html"
                )
                yield ZendeskFileData(
                    identifier=str(article.id),
                    connector_type=self.connector_type,
                    source_identifiers=source_identifiers,
                    additional_metadata=ZendeskAdditionalMetadata(
                        item_type="article", content=article
                    ),
                    metadata=FileDataSourceMetadata(
                        url=str(article.url) if article.url else None,
                        date_created=article.created_at.isoformat() if article.created_at else None,
                        date_modified=(
                            article.updated_at.isoformat() if article.updated_at else None
                        ),
                        date_processed=str(time()),
                    ),
                    display_name=source_identifiers.fullpath,
                )

    async def run_async(self, **kwargs: Any) -> AsyncGenerator[ZendeskFileData, None]:
        """Determines item type and processes accordingly asynchronously."""
        item_type = self.index_config.item_type

        if item_type == "articles":
            async for article_file_data in self.get_articles():
                yield article_file_data

        elif item_type == "tickets":
            async for ticket_file_data in self.get_tickets():
                yield ticket_file_data

        elif item_type == "all":
            async for article_file_data in self.get_articles():
                yield article_file_data
            async for ticket_file_data in self.get_tickets():
                yield ticket_file_data

        else:
            raise ValueError(f"Item type {item_type} is not supported by the indexer")


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

    @requires_dependencies(["aiofiles", "bs4"], extras="zendesk")
    async def download_article(self, article: ZendeskArticle, download_path: Path) -> None:
        import aiofiles
        import bs4

        article_html = article.as_html()
        soup = bs4.BeautifulSoup(article_html, "html.parser")
        async with aiofiles.open(download_path, "w", encoding="utf8") as f:
            await f.write(soup.prettify())

    @requires_dependencies(["aiofiles"], extras="zendesk")
    async def download_ticket(self, ticket: ZendeskTicket, download_path: Path) -> None:
        import aiofiles

        async with aiofiles.open(download_path, "w", encoding="utf8") as f:
            await f.write(ticket.as_text())
            async with self.connection_config.get_client() as client:
                comments = [comment async for comment in client.get_comments(ticket_id=ticket.id)]
                for comment in comments:
                    await f.write(comment.as_text())

    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        zendesk_filedata = ZendeskFileData.cast(file_data=file_data)

        item_type = zendesk_filedata.additional_metadata.item_type
        download_path = self.get_download_path(file_data=zendesk_filedata)
        download_path.parent.mkdir(parents=True, exist_ok=True)

        if item_type == "article":
            article = ZendeskArticle.model_validate(zendesk_filedata.additional_metadata.content)
            await self.download_article(article=article, download_path=download_path)
        elif item_type == "ticket":
            ticket = ZendeskTicket.model_validate(zendesk_filedata.additional_metadata.content)
            await self.download_ticket(ticket=ticket, download_path=download_path)
        else:
            raise RuntimeError(f"Item type {item_type} cannot be handled by the downloader")
        return super().generate_download_response(
            file_data=zendesk_filedata, download_path=download_path
        )


# create entry
zendesk_source_entry = SourceRegistryEntry(
    connection_config=ZendeskConnectionConfig,
    indexer_config=ZendeskIndexerConfig,
    indexer=ZendeskIndexer,
    downloader=ZendeskDownloader,
    downloader_config=ZendeskDownloaderConfig,
)
