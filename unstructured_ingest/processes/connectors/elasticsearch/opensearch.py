from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, Field, Secret, field_validator

from unstructured_ingest.error import (
    DestinationConnectionError,
)
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.processes.connectors.elasticsearch.elasticsearch import (
    ElasticsearchDownloader,
    ElasticsearchDownloaderConfig,
    ElasticsearchIndexer,
    ElasticsearchIndexerConfig,
    ElasticsearchUploader,
    ElasticsearchUploaderConfig,
    ElasticsearchUploadStager,
    ElasticsearchUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from opensearchpy import OpenSearch

CONNECTOR_TYPE = "opensearch"

"""Since the actual OpenSearch project is a fork of Elasticsearch, we are relying
heavily on the Elasticsearch connector code, inheriting the functionality as much as possible."""


class OpenSearchAccessConfig(AccessConfig):
    password: Optional[str] = Field(default=None, description="password when using basic auth")


class OpenSearchClientInput(BaseModel):
    http_auth: Secret[Optional[tuple[str, str]]] = None
    hosts: Optional[list[str]] = None
    use_ssl: bool = False
    verify_certs: bool = False
    ssl_show_warn: bool = False
    ca_certs: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None


class OpenSearchConnectionConfig(ConnectionConfig):
    hosts: Optional[list[str]] = Field(
        default=None,
        description="List of the OpenSearch hosts to connect",
        examples=["http://localhost:9200"],
    )
    username: Optional[str] = Field(default=None, description="username when using basic auth")
    use_ssl: bool = Field(default=False, description="use ssl for the connection")
    verify_certs: bool = Field(default=False, description="whether to verify SSL certificates")
    ssl_show_warn: bool = Field(
        default=False, description="show warning when verify certs is disabled"
    )
    ca_certs: Optional[Path] = Field(default=None, description="path to CA bundle")
    client_cert: Optional[Path] = Field(
        default=None,
        description="path to the file containing the private key and the certificate,"
        " or cert only if using client_key",
    )
    client_key: Optional[Path] = Field(
        default=None,
        description="path to the file containing the private key"
        " if using separate cert and key files",
    )

    access_config: Secret[OpenSearchAccessConfig]

    @field_validator("hosts", mode="before")
    def to_list(cls, value):
        if isinstance(value, str):
            return [value]
        return value

    def get_client_kwargs(self) -> dict:
        # Update auth related fields to conform to what the SDK expects based on the
        # supported methods:
        # https://github.com/opensearch-project/opensearch-py/blob/main/opensearchpy/client/__init__.py
        access_config = self.access_config.get_secret_value()
        client_input_kwargs = {}
        if self.hosts:
            client_input_kwargs["hosts"] = self.hosts
        if self.use_ssl:
            client_input_kwargs["use_ssl"] = self.use_ssl
        if self.verify_certs:
            client_input_kwargs["verify_certs"] = self.verify_certs
        if self.ssl_show_warn:
            client_input_kwargs["ssl_show_warn"] = self.ssl_show_warn
        if self.ca_certs:
            client_input_kwargs["ca_certs"] = str(self.ca_certs)
        if self.client_cert:
            client_input_kwargs["client_cert"] = str(self.client_cert)
        if self.client_key:
            client_input_kwargs["client_key"] = str(self.client_key)
        if self.username and access_config.password:
            client_input_kwargs["http_auth"] = (self.username, access_config.password)
        client_input = OpenSearchClientInput(**client_input_kwargs)
        logger.debug(f"opensearch client inputs mapped to: {client_input.model_dump()}")
        client_kwargs = client_input.model_dump()
        if client_input.http_auth is not None:
            client_kwargs["http_auth"] = client_input.http_auth.get_secret_value()
        client_kwargs = {k: v for k, v in client_kwargs.items() if v is not None}
        return client_kwargs

    @DestinationConnectionError.wrap
    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def get_client(self) -> "OpenSearch":
        from opensearchpy import OpenSearch

        return OpenSearch(**self.get_client_kwargs())


class OpenSearchIndexerConfig(ElasticsearchIndexerConfig):
    pass


@dataclass
class OpenSearchIndexer(ElasticsearchIndexer):
    connection_config: OpenSearchConnectionConfig
    index_config: OpenSearchIndexerConfig
    client: "OpenSearch" = field(init=False)

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def load_scan(self):
        from opensearchpy.helpers import scan

        return scan


class OpenSearchDownloaderConfig(ElasticsearchDownloaderConfig):
    pass


@dataclass
class OpenSearchDownloader(ElasticsearchDownloader):
    connection_config: OpenSearchConnectionConfig
    download_config: OpenSearchDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def load_async(self):
        from opensearchpy import AsyncOpenSearch
        from opensearchpy.helpers import async_scan

        return AsyncOpenSearch, async_scan


class OpenSearchUploaderConfig(ElasticsearchUploaderConfig):
    pass


@dataclass
class OpenSearchUploader(ElasticsearchUploader):
    connection_config: OpenSearchConnectionConfig
    upload_config: OpenSearchUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def load_parallel_bulk(self):
        from opensearchpy.helpers import parallel_bulk

        return parallel_bulk


class OpenSearchUploadStagerConfig(ElasticsearchUploadStagerConfig):
    pass


@dataclass
class OpenSearchUploadStager(ElasticsearchUploadStager):
    upload_stager_config: OpenSearchUploadStagerConfig


opensearch_source_entry = SourceRegistryEntry(
    connection_config=OpenSearchConnectionConfig,
    indexer=OpenSearchIndexer,
    indexer_config=OpenSearchIndexerConfig,
    downloader=OpenSearchDownloader,
    downloader_config=OpenSearchDownloaderConfig,
)


opensearch_destination_entry = DestinationRegistryEntry(
    connection_config=OpenSearchConnectionConfig,
    upload_stager_config=OpenSearchUploadStagerConfig,
    upload_stager=OpenSearchUploadStager,
    uploader_config=OpenSearchUploaderConfig,
    uploader=OpenSearchUploader,
)
