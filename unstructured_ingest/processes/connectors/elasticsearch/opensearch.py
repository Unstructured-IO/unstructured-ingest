import asyncio
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Callable, Optional, Tuple

from pydantic import BaseModel, Field, Secret, field_validator

from unstructured_ingest.data_types.file_data import (
    BatchFileData,
    BatchItem,
    FileData,
    FileDataSourceMetadata,
)
from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
)
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
)
from unstructured_ingest.interfaces.downloader import download_responses
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

"""OpenSearch connector - inherits from Elasticsearch connector (OpenSearch is an ES fork)."""

# Precompiled regex patterns for AWS hostname detection (GovCloud, China, standard)
_ES_PATTERN = re.compile(r"\.([a-z]{2}(?:-[a-z]+)+-\d+)\.es\.amazonaws\.com$")
_AOSS_PATTERN = re.compile(r"^[a-z0-9]+\.([a-z]{2}(?:-[a-z]+)+-\d+)\.aoss\.amazonaws\.com$")


def _run_coroutine(fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
    """Run an async function from sync context, handling existing event loops."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(fn(*args, **kwargs))

    with ThreadPoolExecutor(thread_name_prefix="opensearch") as pool:
        return pool.submit(lambda: asyncio.run(fn(*args, **kwargs))).result()


class OpenSearchAccessConfig(AccessConfig):
    password: Optional[str] = Field(default=None, description="password when using basic auth")
    aws_access_key_id: Optional[str] = Field(
        default=None,
        description="AWS access key ID. When provided (with secret), IAM authentication is used. "
        "Region and service type are auto-detected from the host URL.",
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None,
        description="AWS secret access key. Required when aws_access_key_id is provided.",
    )
    aws_session_token: Optional[str] = Field(
        default=None, description="AWS session token for temporary credentials (optional)"
    )


def detect_aws_opensearch_config(host: str) -> Optional[Tuple[str, str]]:
    """Auto-detect AWS region and service from OpenSearch hostname."""
    clean_host = host.replace("https://", "").replace("http://", "")
    clean_host = clean_host.split(":")[0]

    match = _ES_PATTERN.search(clean_host)
    if match:
        return (match.group(1), "es")

    match = _AOSS_PATTERN.search(clean_host)
    if match:
        return (match.group(1), "aoss")

    return None


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

    def _has_aws_credentials(self) -> bool:
        """Check if AWS IAM credentials are provided."""
        access_config = self.access_config.get_secret_value()
        has_access_key = access_config.aws_access_key_id is not None
        has_secret_key = access_config.aws_secret_access_key is not None

        # Validate: Either both credentials or neither - partial credentials are invalid
        if has_access_key != has_secret_key:  # XOR: exactly one is set
            raise ValueError(
                "AWS IAM authentication requires BOTH aws_access_key_id and aws_secret_access_key. "
                f"Currently provided: aws_access_key_id={'set' if has_access_key else 'not set'}, "
                f"aws_secret_access_key={'set' if has_secret_key else 'not set'}"
            )

        return has_access_key and has_secret_key

    def _detect_and_validate_aws_config(self) -> Tuple[str, str]:
        """Auto-detect AWS region and service from host URL."""
        if not self.hosts:
            raise ValueError("Host is required for AWS OpenSearch connection")

        detected = detect_aws_opensearch_config(self.hosts[0])

        if not detected:
            raise ValueError(
                f"Could not auto-detect AWS region and service from host: {self.hosts[0]}. "
                f"Ensure your host URL follows AWS OpenSearch format: "
                f"https://search-domain-xxx.REGION.es.amazonaws.com (for OpenSearch Service) or "
                f"https://xxx.REGION.aoss.amazonaws.com (for OpenSearch Serverless)"
            )

        region, service = detected
        logger.debug(
            f"Auto-detected AWS configuration from host: region={region}, service={service}"
        )
        return region, service

    @requires_dependencies(["opensearchpy", "boto3"], extras="opensearch")
    async def _get_async_aws_auth(self):
        """Create AWS SigV4 authentication handler for async clients."""
        import boto3
        from opensearchpy import AWSV4SignerAsyncAuth

        access_config = self.access_config.get_secret_value()

        session = boto3.Session(
            aws_access_key_id=access_config.aws_access_key_id,
            aws_secret_access_key=access_config.aws_secret_access_key,
            aws_session_token=access_config.aws_session_token,
        )
        credentials = session.get_credentials()

        if not credentials:
            raise ValueError("Failed to obtain AWS credentials from provided keys")

        return AWSV4SignerAsyncAuth(credentials, *self._detect_and_validate_aws_config())

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    async def get_async_client_kwargs(self) -> dict:
        """Build AsyncOpenSearch client config (auto-detects IAM, basic auth, or cert auth)."""
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

        if self._has_aws_credentials():
            logger.debug("Using AWS IAM authentication")

            # Must use http_async.AsyncHttpConnection for IAM auth handlers
            from opensearchpy.connection.http_async import AsyncHttpConnection

            client_input = OpenSearchClientInput(**client_input_kwargs)
            client_kwargs = client_input.model_dump()
            client_kwargs["http_auth"] = await self._get_async_aws_auth()
            client_kwargs["connection_class"] = AsyncHttpConnection

        elif self.username and access_config.password:
            logger.debug("Using basic HTTP authentication")
            client_input_kwargs["http_auth"] = (self.username, access_config.password)

            client_input = OpenSearchClientInput(**client_input_kwargs)
            client_kwargs = client_input.model_dump()
            if client_input.http_auth:
                client_kwargs["http_auth"] = client_input.http_auth.get_secret_value()

        elif self.client_cert:
            logger.debug("Using certificate-based authentication")
            client_input = OpenSearchClientInput(**client_input_kwargs)
            client_kwargs = client_input.model_dump()

        else:
            logger.warning("No authentication configured - connecting without credentials")
            client_input = OpenSearchClientInput(**client_input_kwargs)
            client_kwargs = client_input.model_dump()

        # Retry and timeout configuration for resilience against transient errors
        client_kwargs["max_retries"] = 3
        client_kwargs["retry_on_status"] = [429, 502, 503]
        client_kwargs["retry_on_timeout"] = True
        client_kwargs["timeout"] = 60

        return {k: v for k, v in client_kwargs.items() if v is not None}


class OpenSearchIndexerConfig(ElasticsearchIndexerConfig):
    pass


@dataclass
class OpenSearchIndexer(ElasticsearchIndexer):
    connection_config: OpenSearchConnectionConfig
    index_config: OpenSearchIndexerConfig
    client: "OpenSearch" = field(init=False)

    def is_async(self) -> bool:
        """Signal pipeline to use async execution."""
        return True

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def precheck(self) -> None:
        """Validate connection and index (sync wrapper required by pipeline framework)."""

        async def _async_precheck():
            from opensearchpy import AsyncOpenSearch

            try:
                async with AsyncOpenSearch(
                    **await self.connection_config.get_async_client_kwargs()
                ) as client:
                    # Use get_alias (GET) instead of exists (HEAD) - HEAD has IAM signing issues
                    # Also respects AWS FGAC by checking only the specific index
                    await client.indices.get_alias(index=self.index_config.index_name)
            except Exception as e:
                logger.error(f"failed to validate connection: {e}", exc_info=True)
                raise SourceConnectionError(f"failed to validate connection: {e}")

        _run_coroutine(_async_precheck)

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    async def run_async(self, **kwargs: Any) -> AsyncGenerator[ElasticsearchBatchFileData, None]:
        """Async indexing for all authentication types."""
        ids = list(await self._get_doc_ids_async())
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

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    async def _get_doc_ids_async(self) -> set[str]:
        """Fetch document IDs using async_scan."""
        from opensearchpy import AsyncOpenSearch
        from opensearchpy.helpers import async_scan

        scan_query = {"stored_fields": [], "query": {"match_all": {}}}

        async with AsyncOpenSearch(
            **await self.connection_config.get_async_client_kwargs()
        ) as client:
            doc_ids = set()
            async for hit in async_scan(
                client,
                query=scan_query,
                scroll="1m",
                index=self.index_config.index_name,
            ):
                doc_ids.add(hit["_id"])
            return doc_ids


class OpenSearchDownloaderConfig(ElasticsearchDownloaderConfig):
    pass


@dataclass
class OpenSearchDownloader(ElasticsearchDownloader):
    connection_config: OpenSearchConnectionConfig
    download_config: OpenSearchDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    async def run_async(self, file_data: BatchFileData, **kwargs: Any) -> download_responses:
        """Download documents from OpenSearch."""
        from opensearchpy import AsyncOpenSearch
        from opensearchpy.helpers import async_scan

        elasticsearch_filedata = ElasticsearchBatchFileData.cast(file_data=file_data)

        index_name: str = elasticsearch_filedata.additional_metadata.index_name
        ids: list[str] = [item.identifier for item in elasticsearch_filedata.batch_items]

        scan_query = {
            "version": True,
            "query": {"ids": {"values": ids}},
        }

        # Only add _source if fields are explicitly specified (avoids AWS FGAC issues)
        if self.download_config.fields:
            scan_query["_source"] = self.download_config.fields

        download_responses = []
        async with AsyncOpenSearch(
            **await self.connection_config.get_async_client_kwargs()
        ) as client:
            async for result in async_scan(
                client,
                query=scan_query,
                scroll="1m",
                index=index_name,
            ):
                download_responses.append(
                    self.generate_download_response(
                        result=result, index_name=index_name, file_data=elasticsearch_filedata
                    )
                )
        return download_responses


class OpenSearchUploaderConfig(ElasticsearchUploaderConfig):
    batch_size_bytes: int = Field(
        default=5_000_000,
        description="Size limit (in bytes) for each batch of items to be uploaded. "
        "Default is 5MB, lower than Elasticsearch default to accommodate "
        "AWS OpenSearch cluster rate limits.",
    )


@dataclass
class OpenSearchUploader(ElasticsearchUploader):
    connection_config: OpenSearchConnectionConfig
    upload_config: OpenSearchUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        """Signal pipeline to use async execution."""
        return True

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def precheck(self) -> None:
        """Validate connection and index (sync wrapper required by pipeline framework)."""

        async def _async_precheck():
            from opensearchpy import AsyncOpenSearch

            try:
                async with AsyncOpenSearch(
                    **await self.connection_config.get_async_client_kwargs()
                ) as client:
                    # Use get_alias (GET) instead of exists (HEAD) - HEAD has IAM signing issues
                    # Also respects AWS FGAC by checking only the specific index
                    await client.indices.get_alias(index=self.upload_config.index_name)
            except Exception as e:
                logger.error(f"failed to validate connection: {e}", exc_info=True)
                raise DestinationConnectionError(f"failed to validate connection: {e}")

        _run_coroutine(_async_precheck)

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    async def run_data_async(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        """Upload data to OpenSearch using async_bulk."""
        from opensearchpy import AsyncOpenSearch
        from opensearchpy.exceptions import TransportError
        from opensearchpy.helpers import async_bulk

        logger.debug(
            f"writing {len(data)} elements to index {self.upload_config.index_name} "
            f"at {self.connection_config.hosts} "
            f"with batch size (bytes) {self.upload_config.batch_size_bytes}"
        )

        async with AsyncOpenSearch(
            **await self.connection_config.get_async_client_kwargs()
        ) as client:
            for batch in generator_batching_wbytes(
                data, batch_size_limit_bytes=self.upload_config.batch_size_bytes
            ):
                # Retry with delay for rate limiting (429 errors)
                max_attempts = 3
                for attempt in range(max_attempts):
                    try:
                        success, failed = await async_bulk(
                            client=client,
                            actions=batch,
                            chunk_size=len(batch),
                            max_chunk_bytes=self.upload_config.batch_size_bytes,
                            raise_on_error=False,
                        )
                        break
                    except Exception as e:
                        # Check for rate limiting: precise type check, then string fallback
                        is_rate_limited = (
                            isinstance(e, TransportError) and e.status_code == 429
                        ) or "429" in str(e) or "too many requests" in str(e).lower()

                        if attempt < max_attempts - 1 and is_rate_limited:
                            logger.warning(
                                f"Rate limited (attempt {attempt + 1}/{max_attempts}), "
                                f"waiting 5s before retry: {e}"
                            )
                            await asyncio.sleep(5)
                        else:
                            logger.error(f"Batch upload failed: {e}")
                            raise DestinationConnectionError(str(e))

                # Check for document failures (outside try to avoid catching our own exception)
                if failed:
                    logger.error(
                        f"Batch upload had {len(failed)} failures out of {len(batch)}. "
                        f"Failed items: {failed[:5]}"
                    )
                    raise DestinationConnectionError(
                        f"Failed to upload {len(failed)} out of {len(batch)} documents"
                    )

                logger.debug(
                    f"uploaded batch of {len(batch)} elements to {self.upload_config.index_name}"
                )

        logger.info(f"Upload complete: {len(data)} elements to {self.upload_config.index_name}")


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
