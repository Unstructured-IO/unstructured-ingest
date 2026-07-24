from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import json
import re
from typing import Any, AsyncGenerator, Generator, Literal, Optional

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

# RediSearch TAG query special characters that must be escaped.
# Includes all tokenizer separators and query-parser metacharacters.
_TAG_ESCAPE_RE = re.compile(r'([,.<>{}\[\]\\\"\'\`:;!@#$%^&*()\-+=~/| /])')


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

    def _resolve_connection_params(self) -> tuple[str, int, str | None, str | None, bool]:
        """Parse URI or direct fields into (host, port, password, username, use_tls)."""
        access_config = self.access_config.get_secret_value()

        if access_config.uri:
            from urllib.parse import urlparse

            parsed = urlparse(access_config.uri)
            if not parsed.hostname:
                raise ValueError("URI is missing a hostname")
            valid_schemes = ("valkey", "valkeys", "redis", "rediss")
            if parsed.scheme not in valid_schemes:
                raise ValueError(
                    f"URI scheme '{parsed.scheme}' is not recognized. "
                    f"Expected one of: {', '.join(valid_schemes)}"
                )
            # Use URI username as fallback if self.username is not set
            username = self.username or parsed.username
            return (
                parsed.hostname,
                parsed.port or 6379,
                parsed.password,
                username,
                parsed.scheme in ("valkeys", "rediss"),
            )
        return (self.host, self.port, access_config.password, self.username, self.ssl)

    @requires_dependencies(["glide"], extras="valkey")
    @asynccontextmanager
    async def create_async_client(self) -> AsyncGenerator:
        from glide import GlideClient, GlideClientConfiguration, NodeAddress

        host, port, password, username, use_tls = self._resolve_connection_params()

        config_kwargs = dict(
            addresses=[NodeAddress(host=host, port=port)],
            use_tls=use_tls,
            client_name="unstructured-ingest-client",
            request_timeout=self.request_timeout,
        )

        if password:
            from glide import ServerCredentials

            creds = (
                ServerCredentials(password=password, username=username)
                if username
                else ServerCredentials(password=password)
            )
            config_kwargs["credentials"] = creds

        config = GlideClientConfiguration(**config_kwargs)
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

        host, port, password, username, use_tls = self._resolve_connection_params()

        config_kwargs = dict(
            addresses=[NodeAddress(host, port)],
            use_tls=use_tls,
            client_name="unstructured-ingest-client",
            request_timeout=self.request_timeout,
        )

        if password:
            from glide_sync import ServerCredentials

            creds = (
                ServerCredentials(password=password, username=username)
                if username
                else ServerCredentials(password=password)
            )
            config_kwargs["credentials"] = creds

        config = GlideClientConfiguration(**config_kwargs)
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
    distance_metric: Literal["COSINE", "L2", "IP"] = Field(
        default="COSINE",
        description="Distance metric for vector search. Options: COSINE, L2, IP.",
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
            logger.error("failed to validate connection", exc_info=True)
            raise DestinationConnectionError(
                f"failed to validate connection to "
                f"{self.connection_config.host or 'configured URI'}"
            )

    def _get_distance_metric(self, module):
        """Resolve distance metric enum from config string."""
        metric_map = {
            "COSINE": module.DistanceMetricType.COSINE,
            "L2": module.DistanceMetricType.L2,
            "IP": module.DistanceMetricType.IP,
        }
        metric_str = self.upload_config.distance_metric.upper()
        if metric_str not in metric_map:
            raise WriteError(
                f"Invalid distance_metric '{self.upload_config.distance_metric}'. "
                f"Must be one of: COSINE, L2, IP"
            )
        return metric_map[metric_str]

    def _build_schema(self, vector_length: int, module):
        """Build FT.CREATE schema using the given module's field types."""
        return [
            module.TextField("text"),
            module.TagField("element_type"),
            module.TagField("source_document"),
            module.TagField("record_id"),
            module.NumericField("page_number"),
            module.VectorField(
                "embedding",
                module.VectorAlgorithm.HNSW,
                module.VectorFieldAttributesHnsw(
                    dimensions=vector_length,
                    distance_metric=self._get_distance_metric(module),
                    type=module.VectorType.FLOAT32,
                ),
            ),
        ]

    @requires_dependencies(["glide_sync"], extras="valkey")
    def create_destination(
        self,
        vector_length: int,
        destination_name: str = "unstructuredautocreated",
        **kwargs: Any,
    ) -> bool:
        import glide_sync as module
        from glide_sync import FtCreateOptions, RequestError, ft

        with self.connection_config.create_sync_client() as client:
            try:
                schema = self._build_schema(vector_length, module)
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
                    raise WriteError(f"Valkey error: {e}") from e

    async def _async_create_destination(self, vector_length: int, client) -> bool:
        import glide as module
        from glide import FtCreateOptions, RequestError, ft

        try:
            schema = self._build_schema(vector_length, module)
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
                raise self._map_glide_error(e) from e

    async def _delete_by_record_id(self, client, file_data: FileData) -> None:
        """Delete all existing keys for this file before re-ingestion.

        Uses FT.SEARCH to find keys tagged with file_data.identifier,
        then deletes them in a loop until all matches are removed. This prevents
        orphaned chunks when a document is re-processed with different chunking.
        """
        from glide import FtSearchLimit, FtSearchOptions, RequestError, ft

        try:
            # Escape all RediSearch TAG special characters
            safe_id = _TAG_ESCAPE_RE.sub(r'\\\1', file_data.identifier)

            query = f"@record_id:{{{safe_id}}}"
            page_size = 10000
            total_deleted = 0

            while True:
                options = FtSearchOptions(limit=FtSearchLimit(offset=0, count=page_size))
                results = await ft.search(client, self.upload_config.index_name, query, options)

                # GLIDE FtSearchResponse: [total_count, {key: {field: value}, ...}]
                if not results or results[0] == 0:
                    break

                docs = results[1] if len(results) > 1 else {}
                if not isinstance(docs, dict) or not docs:
                    break

                keys_to_delete = [
                    key.decode() if isinstance(key, bytes) else key
                    for key in docs.keys()
                ]
                await client.delete(keys_to_delete)
                total_deleted += len(keys_to_delete)

                # If we got fewer results than page_size, we're done
                if len(keys_to_delete) < page_size:
                    break

            if total_deleted > 0:
                logger.debug(
                    f"Deleted {total_deleted} existing keys for "
                    f"record '{file_data.identifier}'"
                )
        except RequestError as e:
            if "no such index" in str(e).lower():
                # Index doesn't exist yet, nothing to delete
                return
            raise self._map_glide_error(e) from e

    async def run_data_async(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        if not data:
            return

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
            index_exists, search_module_available = await self._check_index(client)

            # Fail early if embeddings require the search module but it's not loaded
            if vector_length and not search_module_available:
                raise DestinationConnectionError(
                    "Valkey Search module not loaded but embeddings are present. "
                    "Enable it with: valkey-server --loadmodule /path/to/valkeysearch.so "
                    "OR use the valkey/valkey-bundle Docker image."
                )

            # Delete prior records for this file to prevent orphaned chunks
            if index_exists:
                await self._delete_by_record_id(client, file_data)

            batches = list(batch_generator(data, batch_size=self.upload_config.batch_size))

            if index_exists:
                # Index active — use individual hset to avoid batch timeout
                # caused by synchronous HNSW indexing blocking the pipeline response
                for batch in batches:
                    await self._write_individual(client, batch, file_data, vector_length)
            else:
                # No index — batch writes are fast, create index after
                for batch in batches:
                    await self._write_batch(client, batch, file_data, vector_length)
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
        # Don't re-wrap errors already in our framework hierarchy
        if isinstance(error, (WriteError, DestinationConnectionError)):
            return error

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
            if error_msg.startswith(("noauth", "wrongpass", "noperm")):
                return UserAuthError(message)
            return WriteError(message)
        return WriteError(message)

    def _build_fields(
        self, element: dict, element_id: str, file_data: FileData,
        expected_dim: int | None = None,
    ) -> dict[str, str | bytes]:
        """Build hash fields from an element dict."""
        fields: dict[str, str | bytes] = {
            "text": element.get("text", ""),
            "element_type": element.get("type", ""),
            "source_document": file_data.source_identifiers.filename or "",
            "page_number": str(element.get("metadata", {}).get("page_number", 0)),
            "record_id": file_data.identifier,
            # Store full metadata as JSON for retrieval (not indexed for search)
            "metadata_json": json.dumps(element.get("metadata", {})),
        }

        embeddings = element.get("embeddings")
        if embeddings:
            if not isinstance(embeddings, (list, tuple)):
                raise WriteError(
                    f"Element '{element_id}' has invalid 'embeddings' type: "
                    f"expected list of floats, got {type(embeddings).__name__}"
                )
            vec = np.array(embeddings, dtype=np.float32)
            if vec.ndim != 1:
                raise WriteError(
                    f"Element '{element_id}' has non-flat embeddings (shape={vec.shape}), "
                    f"expected a 1-D list of floats"
                )
            if expected_dim is not None and vec.shape[0] != expected_dim:
                raise WriteError(
                    f"Element '{element_id}' has embedding dimension {vec.shape[0]}, "
                    f"expected {expected_dim} (index schema dimension)"
                )
            fields["embedding"] = vec.tobytes()

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

    async def _write_batch(
        self, client, batch: list[dict], file_data: FileData, vector_length: int | None = None
    ) -> None:
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
                fields = self._build_fields(element, element_id, file_data, expected_dim=vector_length)

                pipeline.hset(key, fields)

                if self.upload_config.ttl_seconds:
                    pipeline.expire(key, self.upload_config.ttl_seconds)

            await client.exec(pipeline, raise_on_error=True)
        except Exception as e:
            raise self._map_glide_error(e) from e

    async def _check_index(self, client) -> tuple[bool, bool]:
        """Check if the FT search index exists and whether the search module is available.

        Returns (index_exists, search_module_available):
        - (True, True): Index exists, module is loaded
        - (False, True): Module loaded but this specific index doesn't exist yet
        - (False, False): ValkeySearch module not loaded (plain Valkey)

        When the module is not available, text-only uploads can proceed;
        vector uploads should fail early (caller's responsibility).
        """
        from glide import RequestError, ft

        try:
            indexes = await ft.list(client)
            return (self.upload_config.index_name.encode() in indexes, True)
        except RequestError as e:
            if "unknown command" in str(e).lower():
                return (False, False)
            raise self._map_glide_error(e) from e

    async def _write_individual(
        self, client, batch: list[dict], file_data: FileData, vector_length: int | None = None
    ) -> None:
        """Write elements one at a time (used when index is active).

        Individual writes avoid the HNSW synchronous indexing timeout that affects
        batch pipelines when an index is active (see valkey-glide #5510, #5704, #6179).
        When TTL is configured, each element uses an atomic 2-command Batch
        (HSET + EXPIRE) to prevent keys without TTL on partial failure.
        """
        from glide import Batch

        try:
            for element in batch:
                element_id = self._validate_element_id(element)
                key = f"{self.upload_config.key_prefix}{element_id}"
                fields = self._build_fields(element, element_id, file_data, expected_dim=vector_length)

                if self.upload_config.ttl_seconds:
                    # Atomic HSET + EXPIRE to prevent TTL-less keys on crash
                    mini_batch = Batch(is_atomic=True)
                    mini_batch.hset(key, fields)
                    mini_batch.expire(key, self.upload_config.ttl_seconds)
                    await client.exec(mini_batch, raise_on_error=True)
                else:
                    await client.hset(key, fields)
        except Exception as e:
            raise self._map_glide_error(e) from e


valkey_destination_entry = DestinationRegistryEntry(
    connection_config=ValkeyConnectionConfig,
    uploader=ValkeyUploader,
    uploader_config=ValkeyUploaderConfig,
)
