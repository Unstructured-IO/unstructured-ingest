from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Generator, Optional

import numpy as np
from pydantic import Field, Secret, model_validator

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.error import DestinationConnectionError, WriteError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    UploaderConfig,
    VectorDBUploader,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies

CONNECTOR_TYPE = "valkey"


class ValkeyAccessConfig(AccessConfig):
    uri: Optional[str] = Field(
        default=None, description="Valkey connection URI (e.g., valkey://host:port)."
    )
    password: Optional[str] = Field(default=None, description="Password for Valkey authentication.")


class ValkeyConnectionConfig(ConnectionConfig):
    access_config: Secret[ValkeyAccessConfig] = Field(
        default=ValkeyAccessConfig(), validate_default=True
    )
    username: Optional[str] = Field(
        default=None, description="Username for Valkey ACL authentication."
    )
    host: Optional[str] = Field(
        default=None, description="Hostname or IP address of a Valkey instance."
    )
    port: Optional[int] = Field(default=6379, description="Port of the Valkey instance.")
    ssl: Optional[bool] = Field(default=True, description="Whether to use TLS for the connection.")
    request_timeout: int = Field(default=30000, description="Request timeout in milliseconds.")
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @model_validator(mode="after")
    def validate_host_or_uri(self) -> "ValkeyConnectionConfig":
        if not self.access_config.get_secret_value().uri:
            if not self.host:
                raise ValueError("Please pass a hostname either directly or through uri")
            if self.port is None:
                raise ValueError("Since URI is not specified, port cannot be None")
            if self.ssl is None:
                raise ValueError("Since URI is not specified, ssl cannot be None")
        return self

    @requires_dependencies(["glide"], extras="valkey")
    @asynccontextmanager
    async def create_async_client(self) -> AsyncGenerator:
        from glide import GlideClient, GlideClientConfiguration, NodeAddress

        access_config = self.access_config.get_secret_value()

        if access_config.uri:
            # Parse URI into host/port for GLIDE (GLIDE doesn't accept URIs directly)
            from urllib.parse import urlparse

            parsed = urlparse(access_config.uri)
            if not parsed.hostname:
                raise ValueError("URI is missing a hostname")
            host = parsed.hostname
            port = parsed.port
            password = parsed.password
            use_tls = parsed.scheme in ("valkeys", "rediss")
        else:
            host = self.host
            port = self.port
            password = access_config.password
            use_tls = self.ssl

        config = GlideClientConfiguration(
            addresses=[NodeAddress(host=host, port=port)],
            use_tls=use_tls,
            client_name="unstructured-ingest-client",
            request_timeout=self.request_timeout,
        )

        if password:
            from glide import ServerCredentials

            username = self.username
            if username:
                config.credentials = ServerCredentials(password=password, username=username)
            else:
                config.credentials = ServerCredentials(password=password)

        client = await GlideClient.create(config)
        try:
            yield client
        finally:
            await client.close()

    @requires_dependencies(["glide_sync"], extras="valkey")
    @contextmanager
    def create_sync_client(self) -> Generator:
        from glide_sync import GlideClient as SyncGlideClient
        from glide_sync import GlideClientConfiguration, NodeAddress

        access_config = self.access_config.get_secret_value()

        if access_config.uri:
            from urllib.parse import urlparse

            parsed = urlparse(access_config.uri)
            if not parsed.hostname:
                raise ValueError("URI is missing a hostname")
            host = parsed.hostname
            port = parsed.port
            password = parsed.password
            use_tls = parsed.scheme in ("valkeys", "rediss")
        else:
            host = self.host
            port = self.port
            password = access_config.password
            use_tls = self.ssl

        config = GlideClientConfiguration(
            addresses=[NodeAddress(host, port)],
            use_tls=use_tls,
            client_name="unstructured-ingest-client",
            request_timeout=self.request_timeout,
        )

        if password:
            from glide_sync import ServerCredentials

            username = self.username
            if username:
                config.credentials = ServerCredentials(password=password, username=username)
            else:
                config.credentials = ServerCredentials(password=password)

        client = SyncGlideClient.create(config)
        try:
            yield client
        finally:
            client.close()


class ValkeyUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=100, description="Number of records per batch.")
    key_prefix: str = Field(
        default="doc:unstructured:",
        description="Prefix for Valkey keys. For cluster mode, use a hash tag "
        "(e.g., '{unstructured}:doc:') to ensure all keys map to the same slot.",
    )
    index_name: str = Field(
        default="unstructured_index", description="Name of the Valkey Search index."
    )
    ttl_seconds: Optional[int] = Field(
        default=None, description="Optional TTL in seconds for uploaded keys."
    )


@dataclass
class ValkeyUploader(VectorDBUploader):
    upload_config: ValkeyUploaderConfig
    connection_config: ValkeyConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        return True

    def precheck(self) -> None:
        try:
            with self.connection_config.create_sync_client() as client:
                client.ping()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["glide"], extras="valkey")
    def create_destination(
        self,
        vector_length: int,
        destination_name: str = "unstructuredautocreated",
        **kwargs: Any,
    ) -> bool:
        from glide_sync import (
            DistanceMetricType,
            FtCreateOptions,
            NumericField,
            RequestError,
            TagField,
            TextField,
            VectorAlgorithm,
            VectorField,
            VectorFieldAttributesHnsw,
            VectorType,
            ft,
        )

        with self.connection_config.create_sync_client() as client:
            try:
                schema = [
                    TextField("text"),
                    TagField("element_type"),
                    TagField("source_document"),
                    NumericField("page_number"),
                    VectorField(
                        "embedding",
                        VectorAlgorithm.HNSW,
                        VectorFieldAttributesHnsw(
                            dimensions=vector_length,
                            distance_metric=DistanceMetricType.COSINE,
                            type=VectorType.FLOAT32,
                        ),
                    ),
                ]

                ft.create(
                    client,
                    self.upload_config.index_name,
                    schema,
                    FtCreateOptions(prefixes=[self.upload_config.key_prefix]),
                )
                logger.info(
                    f"Created Valkey Search index '{self.upload_config.index_name}' "
                    f"with {vector_length}-dim HNSW vector field."
                )
                return True
            except RequestError as e:
                if "Index already exists" in str(e):
                    logger.debug(
                        f"Index '{self.upload_config.index_name}' already exists, "
                        f"skipping creation."
                    )
                    return False
                elif "unknown command" in str(e).lower():
                    raise DestinationConnectionError(
                        "Valkey Search module not loaded. "
                        "Enable it with: valkey-server --loadmodule /path/to/valkeysearch.so "
                        "OR use the valkey/valkey-bundle Docker image."
                    ) from e
                else:
                    raise

    async def _async_create_destination(self, vector_length: int, client) -> bool:
        from glide import (
            DistanceMetricType,
            FtCreateOptions,
            NumericField,
            RequestError,
            TagField,
            TextField,
            VectorAlgorithm,
            VectorField,
            VectorFieldAttributesHnsw,
            VectorType,
            ft,
        )

        try:
            schema = [
                TextField("text"),
                TagField("element_type"),
                TagField("source_document"),
                NumericField("page_number"),
                VectorField(
                    "embedding",
                    VectorAlgorithm.HNSW,
                    VectorFieldAttributesHnsw(
                        dimensions=vector_length,
                        distance_metric=DistanceMetricType.COSINE,
                        type=VectorType.FLOAT32,
                    ),
                ),
            ]

            await ft.create(
                client,
                self.upload_config.index_name,
                schema,
                FtCreateOptions(prefixes=[self.upload_config.key_prefix]),
            )
            logger.info(
                f"Created Valkey Search index '{self.upload_config.index_name}' "
                f"with {vector_length}-dim HNSW vector field."
            )
            return True
        except RequestError as e:
            if "Index already exists" in str(e):
                logger.debug(
                    f"Index '{self.upload_config.index_name}' already exists, skipping creation."
                )
                return False
            elif "unknown command" in str(e).lower():
                raise DestinationConnectionError(
                    "Valkey Search module not loaded. "
                    "Enable it with: valkey-server --loadmodule /path/to/valkeysearch.so "
                    "OR use the valkey/valkey-bundle Docker image."
                ) from e
            else:
                raise

    async def run_data_async(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        # Detect vector dimension
        elements_with_embeddings = [e for e in data if e.get("embeddings")]
        vector_length = (
            len(elements_with_embeddings[0]["embeddings"]) if elements_with_embeddings else None
        )

        logger.info(
            f"Writing {len(data)} elements to Valkey at "
            f"{self.connection_config.host or 'URI'}:{self.connection_config.port or ''}"
        )

        async with self.connection_config.create_async_client() as client:
            index_exists = await self._index_exists(client) if vector_length else False

            batches = list(batch_generator(data, batch_size=self.upload_config.batch_size))

            if index_exists:
                # Index active — use individual hset to avoid batch timeout
                # caused by synchronous HNSW indexing blocking the pipeline response
                for batch in batches:
                    await self._write_individual(client, batch, file_data)
            else:
                # No index (or concurrent creation handled by _async_create_destination's
                # "already exists" guard) — batch writes are fast, create index after
                for batch in batches:
                    await self._write_batch(client, batch, file_data)
                if vector_length:
                    await self._async_create_destination(vector_length, client)

    @staticmethod
    def _map_glide_error(error: Exception) -> Exception:
        """Map GLIDE exceptions to unstructured-ingest error types.

        The unstructured-ingest orchestration layer uses typed errors for retry logic,
        user-facing messages, and status codes. GLIDE raises its own exception hierarchy
        (TimeoutError, ConnectionError, ClosingError, RequestError) which we translate
        here so the framework can distinguish transient failures (timeouts, disconnects)
        from permanent ones (auth errors, bad commands).
        """
        from glide import ClosingError
        from glide import ConnectionError as GlideConnectionError
        from glide import RequestError as GlideRequestError
        from glide import TimeoutError as GlideTimeoutError

        from unstructured_ingest.error import TimeoutError as IngestTimeoutError
        from unstructured_ingest.error import UserAuthError

        message = f"Valkey error: {error}"

        if isinstance(error, GlideTimeoutError):
            return IngestTimeoutError(message)
        if isinstance(error, (GlideConnectionError, ClosingError)):
            return DestinationConnectionError(message)
        if isinstance(error, GlideRequestError):
            error_msg = str(error).lower()
            if "noauth" in error_msg or "wrongpass" in error_msg or "auth" in error_msg:
                return UserAuthError(message)
            return WriteError(message)
        return WriteError(message)

    def _build_fields(
        self, element: dict, element_id: str, file_data: FileData
    ) -> dict[str, str | bytes]:
        """Build hash fields from an element dict."""
        fields: dict[str, str | bytes] = {
            "text": element.get("text", ""),
            "element_type": element.get("type", ""),
            "source_document": file_data.source_identifiers.filename or "",
            "page_number": str(element.get("metadata", {}).get("page_number", 0)),
        }

        embeddings = element.get("embeddings")
        if embeddings:
            if not isinstance(embeddings, (list, tuple)):
                raise WriteError(
                    f"Element '{element_id}' has invalid 'embeddings' type: "
                    f"expected list of floats, got {type(embeddings).__name__}"
                )
            fields["embedding"] = np.array(embeddings, dtype=np.float32).tobytes()

        return fields

    def _validate_element_id(self, element: dict) -> str:
        """Validate and return element_id, raising WriteError if missing."""
        element_id = element.get("element_id")
        if not element_id:
            raise WriteError(
                "Element is missing 'element_id' — cannot construct a unique key. "
                "Ensure data is processed through the Unstructured pipeline."
            )
        return element_id

    async def _write_batch(self, client, batch: list[dict], file_data: FileData) -> None:
        """Write a batch of elements via non-atomic pipeline.

        Uses Batch(is_atomic=False) for performance. If a command fails mid-batch,
        earlier commands are already committed and NOT rolled back. This is acceptable
        because keys are keyed by element_id (idempotent overwrites), and the
        unstructured-ingest orchestration layer retries the full upload on failure.
        """
        from glide import Batch

        try:
            pipeline = Batch(is_atomic=False)
            for element in batch:
                element_id = self._validate_element_id(element)
                key = f"{self.upload_config.key_prefix}{element_id}"
                fields = self._build_fields(element, element_id, file_data)

                pipeline.hset(key, fields)

                if self.upload_config.ttl_seconds:
                    pipeline.expire(key, self.upload_config.ttl_seconds)

            await client.exec(pipeline, raise_on_error=True)
        except Exception as e:
            raise self._map_glide_error(e) from e

    async def _index_exists(self, client) -> bool:
        """Check if the FT search index already exists."""
        from glide import ft

        indexes = await ft.list(client)
        return self.upload_config.index_name.encode() in indexes

    async def _write_individual(self, client, batch: list[dict], file_data: FileData) -> None:
        """Write elements one at a time (used when index is active)."""
        try:
            for element in batch:
                element_id = self._validate_element_id(element)
                key = f"{self.upload_config.key_prefix}{element_id}"
                fields = self._build_fields(element, element_id, file_data)

                await client.hset(key, fields)

                if self.upload_config.ttl_seconds:
                    await client.expire(key, self.upload_config.ttl_seconds)
        except Exception as e:
            raise self._map_glide_error(e) from e


valkey_destination_entry = DestinationRegistryEntry(
    connection_config=ValkeyConnectionConfig,
    uploader=ValkeyUploader,
    uploader_config=ValkeyUploaderConfig,
)
