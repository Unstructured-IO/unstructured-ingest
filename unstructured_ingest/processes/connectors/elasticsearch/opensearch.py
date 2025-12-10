import re
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional, Tuple

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

"""Since the actual OpenSearch project is a fork of Elasticsearch, we are relying
heavily on the Elasticsearch connector code, inheriting the functionality as much as possible."""

# Precompiled regex patterns for AWS OpenSearch hostname detection
_ES_PATTERN = re.compile(r"\.([a-z]{2}-[a-z]+-\d+)\.es\.amazonaws\.com$")
_AOSS_PATTERN = re.compile(r"^[a-z0-9]+\.([a-z]{2}-[a-z]+-\d+)\.aoss\.amazonaws\.com$")


class OpenSearchAccessConfig(AccessConfig):
    password: Optional[str] = Field(default=None, description="password when using basic auth")

    # AWS IAM Authentication - auto-enabled when credentials are provided
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
    """
    Auto-detect AWS region and service from OpenSearch hostname.

    Args:
        host: OpenSearch hostname (e.g., "search-domain.us-west-2.es.amazonaws.com")

    Returns:
        Tuple of (region, service) if detected, None otherwise

    Examples:
        >>> detect_aws_opensearch_config("search-my-domain-xyz.us-west-2.es.amazonaws.com")
        ("us-west-2", "es")

        >>> detect_aws_opensearch_config("abc123.us-east-1.aoss.amazonaws.com")
        ("us-east-1", "aoss")

        >>> detect_aws_opensearch_config("localhost:9200")
        None
    """
    # Clean the host (remove protocol and port)
    clean_host = host.replace("https://", "").replace("http://", "")
    clean_host = clean_host.split(":")[0]  # Remove port if present

    # Pattern 1: AWS OpenSearch Service (es)
    # search-{domain}-{id}.{region}.es.amazonaws.com
    match = _ES_PATTERN.search(clean_host)
    if match:
        region = match.group(1)
        return (region, "es")

    # Pattern 2: OpenSearch Serverless (aoss)
    # {collection-id}.{region}.aoss.amazonaws.com
    match = _AOSS_PATTERN.search(clean_host)
    if match:
        region = match.group(1)
        return (region, "aoss")

    # Not an AWS OpenSearch hostname
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
        """Check if AWS IAM credentials are provided"""
        access_config = self.access_config.get_secret_value()
        return bool(access_config.aws_access_key_id and access_config.aws_secret_access_key)

    def _detect_and_validate_aws_config(self) -> Tuple[str, str]:
        """
        Auto-detect AWS region and service from host URL.

        Returns:
            Tuple of (region, service)

        Raises:
            ValueError: If IAM is enabled but region/service cannot be detected
        """
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
        logger.info(
            f"Auto-detected AWS configuration from host: region={region}, service={service}"
        )
        return region, service

    @requires_dependencies(["opensearchpy", "boto3"], extras="opensearch")
    async def _get_async_aws_auth(self):
        """
        Create AWS SigV4 authentication handler for asynchronous clients.

        Returns:
            AWSV4SignerAsyncAuth: Async auth handler with auto-detected region/service

        Raises:
            ValueError: If region/service cannot be auto-detected or credentials invalid
        """
        import boto3

        access_config = self.access_config.get_secret_value()

        # Auto-detect AWS region and service from host URL
        aws_region, aws_service = self._detect_and_validate_aws_config()

        # Create boto3 session with explicit credentials
        logger.debug("Creating AWS session with explicit credentials for async client")
        session = boto3.Session(
            aws_access_key_id=access_config.aws_access_key_id,
            aws_secret_access_key=access_config.aws_secret_access_key,
            aws_session_token=access_config.aws_session_token,
        )
        credentials = session.get_credentials()

        if not credentials:
            raise ValueError("Failed to obtain AWS credentials from provided keys")

        # Create async auth handler
        logger.debug(f"Using AWSV4SignerAsyncAuth for region={aws_region}, service={aws_service}")
        from opensearchpy import AWSV4SignerAsyncAuth

        return AWSV4SignerAsyncAuth(credentials, aws_region, aws_service)

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    async def get_async_client_kwargs(self) -> dict:
        """
        Build complete client configuration for AsyncOpenSearch with all authentication types.

        Authentication priority order (auto-detected):
        1. AWS IAM (if aws_access_key_id + aws_secret_access_key provided)
        2. Basic HTTP Auth (if username + password)
        3. SSL Certificates (if cert files provided)

        Returns:
            dict: Complete configuration for AsyncOpenSearch client

        Raises:
            ValueError: If no authentication method is configured
        """
        access_config = self.access_config.get_secret_value()
        client_input_kwargs = {}

        # 1. Configure hosts
        if self.hosts:
            client_input_kwargs["hosts"] = self.hosts

        # 2. Configure SSL/TLS
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

        # 3. Configure Authentication (auto-detect based on credentials provided)
        # Priority order: IAM > Basic > Cert
        if self._has_aws_credentials():
            # AWS IAM Authentication with async auth handler
            logger.info("Using AWS IAM authentication")
            iam_auth = await self._get_async_aws_auth()

            # Must use AsyncHttpConnection for IAM
            from opensearchpy import AsyncHttpConnection

            # Skip Pydantic validation for IAM (auth object, not tuple)
            # Validate non-auth fields only
            client_input = OpenSearchClientInput(**client_input_kwargs)
            logger.debug(f"opensearch client inputs mapped to: {client_input.model_dump()}")

            client_kwargs = client_input.model_dump()
            # Add IAM auth after validation
            client_kwargs["http_auth"] = iam_auth
            client_kwargs["connection_class"] = AsyncHttpConnection

        elif self.username and access_config.password:
            # Basic HTTP Authentication (works in async)
            logger.info("Using basic HTTP authentication")
            client_input_kwargs["http_auth"] = (self.username, access_config.password)

            # Validate through Pydantic for basic auth
            client_input = OpenSearchClientInput(**client_input_kwargs)
            logger.debug(f"opensearch client inputs mapped to: {client_input.model_dump()}")

            client_kwargs = client_input.model_dump()
            if client_input.http_auth is not None:
                client_kwargs["http_auth"] = client_input.http_auth.get_secret_value()

        elif self.client_cert:
            # Certificate-based authentication (works in async)
            logger.info("Using certificate-based authentication")

            # Validate through Pydantic
            client_input = OpenSearchClientInput(**client_input_kwargs)
            logger.debug(f"opensearch client inputs mapped to: {client_input.model_dump()}")

            client_kwargs = client_input.model_dump()

        else:
            raise ValueError(
                "No authentication configured. Must provide one of: "
                "AWS credentials (aws_access_key_id + aws_secret_access_key), "
                "Basic auth (username + password), "
                "or Certificate (client_cert)"
            )

        # Filter out None values
        client_kwargs = {k: v for k, v in client_kwargs.items() if v is not None}

        return client_kwargs


class OpenSearchIndexerConfig(ElasticsearchIndexerConfig):
    pass


@dataclass
class OpenSearchIndexer(ElasticsearchIndexer):
    connection_config: OpenSearchConnectionConfig
    index_config: OpenSearchIndexerConfig
    client: "OpenSearch" = field(init=False)

    def is_async(self) -> bool:
        """Always use async for better I/O performance"""
        return True

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def precheck(self) -> None:
        """Validate connection and index existence (wraps async implementation)"""
        import asyncio

        async def _async_precheck():
            from opensearchpy import AsyncOpenSearch

            try:
                client_kwargs = await self.connection_config.get_async_client_kwargs()
                async with AsyncOpenSearch(**client_kwargs) as client:
                    indices = await client.indices.get_alias(index="*")
                    if self.index_config.index_name not in indices:
                        index_list = ", ".join(indices.keys())
                        raise SourceConnectionError(
                            f"index {self.index_config.index_name} not found: {index_list}"
                        )
                    logger.info(
                        f"Successfully validated connection to index: "
                        f"{self.index_config.index_name}"
                    )
            except Exception as e:
                logger.error(f"failed to validate connection: {e}", exc_info=True)
                raise SourceConnectionError(f"failed to validate connection: {e}")

        # Run async precheck synchronously
        asyncio.run(_async_precheck())

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    async def run_async(self, **kwargs: Any) -> AsyncGenerator[ElasticsearchBatchFileData, None]:
        """Async indexing for all authentication types"""
        all_ids = await self._get_doc_ids_async()
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

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    async def _get_doc_ids_async(self) -> set[str]:
        """Fetch document IDs asynchronously using async_scan"""
        from opensearchpy import AsyncOpenSearch
        from opensearchpy.helpers import async_scan

        scan_query = {"stored_fields": [], "query": {"match_all": {}}}

        # Get async client kwargs with proper authentication
        client_kwargs = await self.connection_config.get_async_client_kwargs()

        async with AsyncOpenSearch(**client_kwargs) as client:
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
        """Override to use async client kwargs with IAM auth"""
        from opensearchpy import AsyncOpenSearch
        from opensearchpy.helpers import async_scan

        elasticsearch_filedata = ElasticsearchBatchFileData.cast(file_data=file_data)

        index_name: str = elasticsearch_filedata.additional_metadata.index_name
        ids: list[str] = [item.identifier for item in elasticsearch_filedata.batch_items]

        scan_query = {
            "version": True,
            "query": {"ids": {"values": ids}},
        }

        # Only add _source if fields are explicitly specified
        if self.download_config.fields:
            scan_query["_source"] = self.download_config.fields

        download_responses = []
        # Use async client kwargs with proper authentication
        client_kwargs = await self.connection_config.get_async_client_kwargs()
        async with AsyncOpenSearch(**client_kwargs) as client:
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
    pass


@dataclass
class OpenSearchUploader(ElasticsearchUploader):
    connection_config: OpenSearchConnectionConfig
    upload_config: OpenSearchUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        """Declare this uploader as async-capable"""
        return True

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    def precheck(self) -> None:
        """Validate connection and index existence (wraps async implementation)"""
        import asyncio

        async def _async_precheck():
            from opensearchpy import AsyncOpenSearch

            try:
                client_kwargs = await self.connection_config.get_async_client_kwargs()
                async with AsyncOpenSearch(**client_kwargs) as client:
                    indices = await client.indices.get_alias(index="*")
                    if self.upload_config.index_name not in indices:
                        index_list = ", ".join(indices.keys())
                        raise DestinationConnectionError(
                            f"index {self.upload_config.index_name} not found: {index_list}"
                        )
                    logger.info(
                        f"Successfully validated connection to index: "
                        f"{self.upload_config.index_name}"
                    )
            except Exception as e:
                logger.error(f"failed to validate connection: {e}", exc_info=True)
                raise DestinationConnectionError(f"failed to validate connection: {e}")

        # Run async precheck synchronously
        asyncio.run(_async_precheck())

    @requires_dependencies(["opensearchpy"], extras="opensearch")
    async def run_data_async(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        """
        Upload data to OpenSearch asynchronously using async_bulk.

        Args:
            data: List of documents to upload
            file_data: File metadata
            **kwargs: Additional arguments

        Raises:
            DestinationConnectionError: If upload fails
        """
        from opensearchpy import AsyncOpenSearch
        from opensearchpy.helpers import async_bulk
        from opensearchpy.helpers.errors import BulkIndexError

        upload_destination = self.connection_config.hosts
        logger.info(
            f"writing {len(data)} elements asynchronously to "
            f"index {self.upload_config.index_name} at {upload_destination} with "
            f"batch size (in bytes) {self.upload_config.batch_size_bytes}"
        )

        # Get async client with proper authentication
        client_kwargs = await self.connection_config.get_async_client_kwargs()

        async with AsyncOpenSearch(**client_kwargs) as client:
            # Check if index exists
            index_exists = await client.indices.exists(index=self.upload_config.index_name)
            if not index_exists:
                logger.warning(
                    f"Index {self.upload_config.index_name} does not exist. "
                    f"This may cause upload issues."
                )

            # Upload in batches
            for batch in generator_batching_wbytes(
                data, batch_size_limit_bytes=self.upload_config.batch_size_bytes
            ):
                try:
                    # Use async_bulk for non-blocking upload
                    success, failed = await async_bulk(
                        client=client,
                        actions=batch,
                        chunk_size=len(batch),
                        max_chunk_bytes=self.upload_config.batch_size_bytes,
                        raise_on_error=False,
                    )

                    logger.info(
                        f"uploaded batch of {len(batch)} elements to index "
                        f"{self.upload_config.index_name}: "
                        f"{success} succeeded, {len(failed) if failed else 0} failed"
                    )

                    if failed:
                        logger.error(f"Failed items: {failed[:5]}")  # Log first 5 failures

                except BulkIndexError as e:
                    sanitized_errors = [
                        self._sanitize_bulk_index_error(error) for error in e.errors
                    ]
                    logger.error(f"Batch upload failed: {e} - errors: {sanitized_errors}")
                    raise DestinationConnectionError(str(e))
                except Exception as e:
                    logger.error(f"Batch upload failed: {e}", exc_info=True)
                    raise DestinationConnectionError(f"Upload failed: {e}")


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
