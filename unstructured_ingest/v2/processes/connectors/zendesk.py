from __future__ import annotations

import asyncio
import json 
from dataclasses import dataclass
from pathlib import Path
from time import time 
from typing import TYPE_CHECKING, Any, AsyncIterator, Optional, Generator

from dateutil import parser
from pydantic import Field, Secret

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
    access_config: Secret[ZendeskAccessConfig]

    @requires_dependencies(["zenpy"], extras="zenpy")
    @contextmanager
    def get_client(self) -> Generator["ZendeskAPI", None, None]:
        pass