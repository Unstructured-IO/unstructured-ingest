from __future__ import annotations

import asyncio
import json 
from dataclasses import dataclass
from pathlib import Path
from time import time 
from typing import TYPE_CHECKING, Any, AsyncIterator, Optional, Generator
import io 

from dateutil import parser
from pydantic import Field, Secret

from contextlib import contextmanager


from unstructured.errors import (
    SourceConnectionError,
    SourceConnectionNetworkError
)


from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig, 
    BatchFileData,
    Downloader, 
    DownloaderConfig, 
    DownloadResponse, 
    FileData, 
    FileDataSourceMetadata, 
    Indexer, 
    IndexerConfig, 
    SourceIdentifiers,
    Uploader, 
    UploaderConfig
)

from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    SourceRegistryEntry
)


if TYPE_CHECKING: 
    from zenpy import Zenpy
    from zenpy.lib.api_objects.chat_objects import Tickets

CONNECTOR_TYPE = "zendesk"

class ZendeskAccessConfig(AccessConfig):
    api_token: str = Field(description="API token for zendesk generated under Apps and Integrations")

class ZendeskConnectionConfig(ConnectionConfig):
    sub_domain: str = Field(description="Subdomain for zendesk site, <sub-domain>.company.com")
    email: str = Field(description ="Email for zendesk site registered at the subdomain")
    access_config: Secret[ZendeskAccessConfig]

    @requires_dependencies(["zenpy"], extras="zenpy")
    @contextmanager
    def get_client(self) -> Generator["Zenpy", None, None]:
        access_config = self.access_config.get_secret_value()
        
        options = {
            'subdomain': self.sub_domain,
            'email' : self.email, 
            'token': access_config.api_token,
        }

        with Zenpy(**options) as client: 
            yield client


class ZendeskIndexerConfig(IndexerConfig):
    batch_size: int = Field(default="10", description="Number of tickets")

@dataclass
class ZendeskIndexer(Indexer):
    connection_config: ZendeskConnectionConfig
    index_config: ZendeskIndexerConfig
    connector_type: str = CONNECTOR_TYPE


    def precheck(self) -> None:
        """Validates connection to Zendesk api"""
        try: 
            with self.connection_config.get_client() as client: 
                # there needs to be at least one user
                if client.users()[:] == []:
                    raise SourceConnectionError(
                        f"users do not exist in zendesk subdomain {self.connection_config.sub_domain}"
                    ) 

        except Exception as e: 
            logger.error(f'Failed to validate connection to Zendesk: {e}', exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")

    def run_async(self, **kwargs):
        return NotImplementedError
    
    def is_async(self) -> bool:
        return False

    def _list_tickets(self):
        with self.connection_config.get_client() as client: 
            tickets = client.tickets()
            return tickets

    def _list_comments(self, ticket_generator, ticket_id: int):
        return ticket_generator.comments(ticket=ticket_id)

    # require dependency zenpy
    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        """Generates FileData objects for each ticket"""
        
        ticket_generator = self._list_tickets()

        # need to handle indexing here for now. 
            
        return ticket_generator

class ZendeskDownloaderConfig(DownloaderConfig):
    pass 

@dataclass 
class ZendeskDownloader(Downloader):

    def _write_file(self, file_data: FileData, file_contents: io.BytesIO) -> DownloadResponse:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"writing {file_data.source_identifiers.fullpath} to {download_path}")
        with open(download_path, "wb") as handler:
            handler.write(file_contents.getbuffer())
        return self.generate_download_response(file_data=file_data, download_path=download_path)
    

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse: 

        logger.debug(f"fetching file: {file_data.source_identifiers.fullpath}")
        file_contents = io.BytesIO() # placeholder. 
        return self._write_file(file_data=file_data, file_contents=file_contents)



# create entry 
zendesk_source_entry = SourceRegistryEntry(
    connection_config=ZendeskConnectionConfig,
    indexer_config=ZendeskIndexerConfig,
    indexer=ZendeskIndexer,
    downloader=ZendeskDownloader,
    downloader_config=ZendeskDownloaderConfig,
)