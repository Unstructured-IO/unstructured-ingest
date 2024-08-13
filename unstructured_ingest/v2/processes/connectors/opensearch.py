from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, Secret

from unstructured_ingest.error import (
    DestinationConnectionError,
)
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.elasticsearch import (
    ElasticsearchDownloader,
    ElasticsearchDownloaderConfig,
    ElasticsearchIndexer,
    ElasticsearchIndexerConfig,
    ElasticsearchUploader,
    ElasticsearchUploaderConfig,
    ElasticsearchUploadStager,
    ElasticsearchUploadStagerConfig,
)

if TYPE_CHECKING:
    from opensearchpy import OpenSearch

CONNECTOR_TYPE = "opensearch"

"""Since the actual OpenSearch project is a fork of Elasticsearch, we are relying
heavily on the Elasticsearch connector code, inheriting the functionality as much as possible."""


class OpenSearchAccessConfig(AccessConfig):
    password: Optional[str] = None
    use_ssl: bool = False
    verify_certs: bool = False
    ssl_show_warn: bool = False
    ca_certs: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None


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
    hosts: Optional[list[str]] = None
    username: Optional[str] = None
    access_config: Secret[OpenSearchAccessConfig]

    def get_client_kwargs(self) -> dict:
        # Update auth related fields to conform to what the SDK expects based on the
        # supported methods:
        # https://github.com/opensearch-project/opensearch-py/blob/main/opensearchpy/client/__init__.py
        access_config = self.access_config.get_secret_value()
        client_input_kwargs = {}
        if self.hosts:
            client_input_kwargs["hosts"] = self.hosts
        if access_config.use_ssl:
            client_input_kwargs["use_ssl"] = access_config.use_ssl
        if access_config.verify_certs:
            client_input_kwargs["verify_certs"] = access_config.verify_certs
        if access_config.ssl_show_warn:
            client_input_kwargs["ssl_show_warn"] = access_config.ssl_show_warn
        if access_config.ca_certs:
            client_input_kwargs["ca_certs"] = access_config.ca_certs
        if access_config.client_cert:
            client_input_kwargs["client_cert"] = access_config.client_cert
        if access_config.client_key:
            client_input_kwargs["client_key"] = access_config.client_key
        if self.username and access_config.password:
            client_input_kwargs["http_auth"] = (self.username, access_config.password)
        client_input = OpenSearchClientInput(**client_input_kwargs)
        logger.debug(f"OpenSearch client inputs mapped to: {client_input.dict()}")
        client_kwargs = client_input.dict()
        client_kwargs["http_auth"] = client_input.http_auth.get_secret_value()
        client_kwargs = {k: v for k, v in client_kwargs.items() if v is not None}
        return client_kwargs

    @DestinationConnectionError.wrap
    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def get_client(self) -> "OpenSearch":
        from opensearchpy import OpenSearch

        return OpenSearch(**self.get_client_kwargs())


class OpensearchIndexerConfig(ElasticsearchIndexerConfig):
    pass


@dataclass
class OpenSearchIndexer(ElasticsearchIndexer):
    connection_config: OpenSearchConnectionConfig
    index_config: OpensearchIndexerConfig
    client: "OpenSearch" = field(init=False)

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def load_scan(self):
        from opensearchpy.helpers import scan

        return scan


class OpensearchDownloaderConfig(ElasticsearchDownloaderConfig):
    pass


@dataclass
class OpenSearchDownloader(ElasticsearchDownloader):
    connection_config: OpenSearchConnectionConfig
    download_config: OpensearchDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def load_async(self):
        from opensearchpy import AsyncOpenSearch
        from opensearchpy.helpers import async_scan

        return AsyncOpenSearch, async_scan


class OpensearchUploaderConfig(ElasticsearchUploaderConfig):
    pass


@dataclass
class OpenSearchUploader(ElasticsearchUploader):
    connection_config: OpenSearchConnectionConfig
    upload_config: OpensearchUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def load_parallel_bulk(self):
        from opensearchpy.helpers import parallel_bulk

        return parallel_bulk


class OpensearchUploadStagerConfig(ElasticsearchUploadStagerConfig):
    pass


@dataclass
class OpensearchUploadStager(ElasticsearchUploadStager):
    upload_stager_config: OpensearchUploadStagerConfig


opensearch_source_entry = SourceRegistryEntry(
    connection_config=OpenSearchConnectionConfig,
    indexer=OpenSearchIndexer,
    indexer_config=OpensearchIndexerConfig,
    downloader=OpenSearchDownloader,
    downloader_config=OpensearchDownloaderConfig,
)


opensearch_destination_entry = DestinationRegistryEntry(
    connection_config=OpenSearchConnectionConfig,
    upload_stager_config=OpensearchUploadStagerConfig,
    upload_stager=OpensearchUploadStager,
    uploader_config=OpensearchUploaderConfig,
    uploader=OpenSearchUploader,
)
