import collections
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import BaseModel, Field, Secret, field_validator

from unstructured_ingest.data_types.file_data import (
    BatchItem,
    FileData,
    FileDataSourceMetadata,
)
from unstructured_ingest.error import (
    DestinationConnectionError,
    UnstructuredIngestError,
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
    ElasticsearchBatchFileData,
    ElasticsearchDownloader,
    ElasticsearchDownloaderConfig,
    ElasticsearchIndexer,
    ElasticsearchIndexerConfig,
    ElasticsearchUploader,
    ElasticsearchUploaderConfig,
    ElasticsearchUploadStager,
    ElasticsearchUploadStagerConfig,
    ElastisearchAdditionalMetadata,
)
from unstructured_ingest.utils.data_prep import batch_generator, generator_batching_wbytes
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
    hosts: list[str] = Field(
        ...,
        min_length=1,
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
    @classmethod
    def validate_hosts(cls, value):
        if isinstance(value, str):
            value = [value]
        if not value:
            raise ValueError("At least one OpenSearch host must be provided. ")
        for host in value:
            if not host or not host.strip():
                raise ValueError("Host URL cannot be empty")
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

    def run(self, **kwargs: Any) -> Generator[ElasticsearchBatchFileData, None, None]:
        all_ids = self._get_doc_ids()
        ids = list(all_ids)
        for batch in batch_generator(ids, self.index_config.batch_size):
            batch_items = [BatchItem(identifier=b) for b in batch]
            url = f"{self.connection_config.hosts[0]}/{self.index_config.index_name}"
            display_name = (
                f"url={url}, batch_size={len(batch_items)} "
                f"ids={batch_items[0].identifier}..{batch_items[-1].identifier}"
            )
            yield ElasticsearchBatchFileData(
                connector_type=CONNECTOR_TYPE,
                metadata=FileDataSourceMetadata(
                    url=url,
                    date_processed=str(time()),
                ),
                additional_metadata=ElastisearchAdditionalMetadata(
                    index_name=self.index_config.index_name,
                ),
                batch_items=batch_items,
                display_name=display_name,
            )


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

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        """OpenSearch-specific implementation without index existence check."""
        from opensearchpy.helpers.errors import BulkIndexError

        parallel_bulk = self.load_parallel_bulk()
        upload_destination = self.connection_config.hosts

        logger.info(
            f"writing {len(data)} elements via document batches to destination "
            f"index named {self.upload_config.index_name} at {upload_destination} with "
            f"batch size (in bytes) {self.upload_config.batch_size_bytes} with "
            f"{self.upload_config.num_threads} (number of) threads"
        )

        with self.connection_config.get_client() as client:
            for batch in generator_batching_wbytes(
                data, batch_size_limit_bytes=self.upload_config.batch_size_bytes
            ):
                try:
                    iterator = parallel_bulk(
                        client=client,
                        actions=batch,
                        thread_count=self.upload_config.num_threads,
                    )
                    collections.deque(iterator, maxlen=0)
                    logger.info(
                        f"uploaded batch of {len(batch)} elements to index "
                        f"{self.upload_config.index_name}"
                    )
                except BulkIndexError as e:
                    sanitized_errors = [
                        self._sanitize_bulk_index_error(error) for error in e.errors
                    ]
                    logger.error(
                        f"Batch upload failed - {e} - with following errors: {sanitized_errors}"
                    )
                    raise DestinationConnectionError(str(e))
                except Exception as e:
                    logger.error(f"Batch upload failed - {e}")
                    raise UnstructuredIngestError(str(e))


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
