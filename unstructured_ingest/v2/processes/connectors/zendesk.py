from __future__ import annotations

import asyncio
import json 
from dataclasses import dataclass
from pathlib import Path
from time import time 
from typing import TYPE_CHECKING, Any, AsyncIterator, Optional, Generator

from dateutil import parser
from pydantic import Field, Secret

from contextlib import contextmanager


from unstructured.errors import (
    SourceConnectionError,
    SourceConnectionNetworkError
)

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
    batch_size: int = Field(default="10", description="Number of tickets ")
    ticket_classification: str = Field(default="Open", description="Tickets can be either open or closed, if `Comment` is selected, it will only pursue comments of open/closed tickets")

