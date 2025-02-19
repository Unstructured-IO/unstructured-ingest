from __future__ import annotations

import datetime
import hashlib
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import Any, Generator, List

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

from .wrapper import Comment, ZendeskClient, ZendeskTicket

CONNECTOR_TYPE = "zendesk"


class ZendeskAccessConfig(AccessConfig):
    api_token: str = Field(
        description="API token for zendesk generated under Apps and Integrations"
    )


class ZendeskConnectionConfig(ConnectionConfig):
    sub_domain: str = Field(description="Subdomain for zendesk site, <sub-domain>.company.com")
    email: str = Field(description="Email for zendesk site registered at the subdomain")
    access_config: Secret[ZendeskAccessConfig]

    @contextmanager
    def get_client(self) -> Generator["ZendeskClient", None, None]:

        access_config = self.access_config.get_secret_value()

        options = {
            "email": self.email,
            "subdomain": self.sub_domain,
            "token": access_config.api_token,
        }

        client = ZendeskClient(**options)
        yield client


class ZendeskIndexerConfig(IndexerConfig):
    batch_size: int = Field(
        default=1,
        description="[NotImplemented]Number of tickets: Currently batching is not supported",
    )


@dataclass
class ZendeskIndexer(Indexer):
    connection_config: ZendeskConnectionConfig
    index_config: ZendeskIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        """Validates connection to Zendesk api"""
        try:
            # there is no context manager method for Zenpy.
            with self.connection_config.get_client() as client:

                if client.get_users() == []:
                    subdomain_endpoint = f"{self.connection_config.sub_domain}.zendesk.com"
                    raise SourceConnectionError(
                        f"users do not exist in subdomain {subdomain_endpoint}"
                    )

        except Exception as e:
            logger.error(f"Failed to validate connection to Zendesk: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")

    def run_async(self, **kwargs):
        return NotImplementedError

    def is_async(self) -> bool:
        return False

    def _list_tickets(self) -> List[ZendeskTicket]:
        with self.connection_config.get_client() as client:
            tickets = client.get_tickets()
            return tickets

    def _list_comments(self, ticket_generator, ticket_id: int) -> List[Comment]:
        return ticket_generator.get_comments(ticket_id)

    def _generate_fullpath(self, ticket: ZendeskTicket) -> Path:
        return Path(hashlib.sha256(str(ticket.id).encode("utf-8")).hexdigest()[:16] + ".txt")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        """Generates FileData objects for each ticket"""
        ticket_generator = self._list_tickets()

        for ticket in ticket_generator:

            metadata = FileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={"id": str(ticket.id), "subject": str(ticket.subject)},
            )

            full_path = self._generate_fullpath(ticket)

            source_identifiers = SourceIdentifiers(filename=full_path.name, fullpath=str(full_path))

            file_data = FileData(
                identifier=str(ticket.id),
                connector_type=self.connector_type,
                metadata=metadata,
                source_identifiers=source_identifiers,
            )

            yield file_data


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

            # handle filedata bs
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
