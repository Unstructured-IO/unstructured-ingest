import asyncio
import json
from pathlib import Path

import numpy as np
import pytest

from test.integration.connectors.utils.constants import DESTINATION_TAG, NOSQL_TAG
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.valkey import (
    CONNECTOR_TYPE as VALKEY_CONNECTOR_TYPE,
)
from unstructured_ingest.processes.connectors.valkey import (
    ValkeyAccessConfig,
    ValkeyConnectionConfig,
    ValkeyUploader,
    ValkeyUploaderConfig,
)

VALKEY_TEST_HOST = "127.0.0.1"
VALKEY_TEST_PORT = 6379


def _run_async(coro):
    """Run a coroutine in a fresh event loop via asyncio.run().

    GLIDE's Batch.exec() has an incompatibility with pytest-asyncio's event loop
    management that causes batch operations to hang. Running in a fresh loop via
    asyncio.run() avoids this issue while keeping the Batch pipeline in production code.
    """
    return asyncio.run(coro)


async def get_test_client():
    """Create a GLIDE client for test validation."""
    from glide import GlideClient, GlideClientConfiguration, NodeAddress

    config = GlideClientConfiguration(
        addresses=[NodeAddress(host=VALKEY_TEST_HOST, port=VALKEY_TEST_PORT)],
        request_timeout=10000,
    )
    return await GlideClient.create(config)


async def cleanup_keys(keys: list[str]) -> None:
    """Delete test keys after test."""
    client = await get_test_client()
    try:
        for key in keys:
            await client.delete([key])
    finally:
        await client.close()


def _cleanup(keys: list[str], index_name: str | None = None):
    """Shared test cleanup: delete keys and optionally drop index."""
    async def _do_cleanup():
        await cleanup_keys(keys)
        if index_name:
            try:
                client = await get_test_client()
                from glide import ft

                await ft.dropindex(client, index_name)
                await client.close()
            except Exception:
                pass

    _run_async(_do_cleanup())


async def validate_upload(element: dict, key_prefix: str) -> None:
    """Validate that an element was correctly stored in Valkey."""
    client = await get_test_client()
    try:
        key = f"{key_prefix}{element['element_id']}"
        result = await client.hgetall(key)
        assert result is not None
        assert len(result) > 0

        # Decode bytes to strings for comparison
        decoded = {k.decode(): v for k, v in result.items()}

        assert decoded["text"].decode() == element.get("text", "")
        assert decoded["element_type"].decode() == element.get("type", "")

        # Validate vector embedding if present
        if element.get("embeddings"):
            stored_embedding = np.frombuffer(decoded["embedding"], dtype=np.float32)
            expected_embedding = np.array(element["embeddings"], dtype=np.float32)
            similarity = np.linalg.norm(stored_embedding - expected_embedding)
            assert similarity < 1e-5
    finally:
        await client.close()


async def _upload_and_validate(
    upload_file: Path, key_prefix: str, index_name: str, **uploader_kwargs
):
    """Core upload logic run in a fresh event loop."""
    uploader = ValkeyUploader(
        connection_config=ValkeyConnectionConfig(
            host=VALKEY_TEST_HOST,
            port=VALKEY_TEST_PORT,
            ssl=False,
            access_config=ValkeyAccessConfig(),
        ),
        upload_config=ValkeyUploaderConfig(
            batch_size=10,
            key_prefix=key_prefix,
            index_name=index_name,
            **uploader_kwargs,
        ),
    )

    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=VALKEY_CONNECTOR_TYPE,
        identifier="mock-file-data",
    )

    with upload_file.open() as f:
        elements = json.load(f)

    await uploader.run_data_async(data=elements, file_data=file_data)
    return elements


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_upload(upload_file: Path, tmp_path: Path):
    """Test uploading elements to local Valkey with vector index creation."""
    key_prefix = "test:unstructured:"
    index_name = "test_unstructured_index"

    async def run():
        elements = await _upload_and_validate(upload_file, key_prefix, index_name)
        await validate_upload(elements[0], key_prefix)
        return elements

    try:
        elements = _run_async(run())
    finally:
        keys = [f"{key_prefix}{e['element_id']}" for e in json.loads(upload_file.read_text())]
        _cleanup(keys, index_name)


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_with_ttl(upload_file: Path, tmp_path: Path):
    """Test that TTL is applied to uploaded keys."""
    key_prefix = "test:ttl:"
    index_name = "test_ttl_index"

    async def run():
        elements = await _upload_and_validate(upload_file, key_prefix, index_name, ttl_seconds=3600)

        # Verify TTL is set
        client = await get_test_client()
        try:
            first_key = f"{key_prefix}{elements[0]['element_id']}"
            ttl = await client.ttl(first_key)
            assert ttl > 0, f"Expected TTL > 0, got {ttl}"
        finally:
            await client.close()

        return elements

    try:
        elements = _run_async(run())
    finally:
        keys = [f"{key_prefix}{e['element_id']}" for e in json.loads(upload_file.read_text())]
        _cleanup(keys, index_name)


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_precheck_failure():
    """Test that precheck raises DestinationConnectionError on bad connection."""
    from unstructured_ingest.error import DestinationConnectionError

    uploader = ValkeyUploader(
        connection_config=ValkeyConnectionConfig(
            host="nonexistent-host",
            port=9999,
            ssl=False,
            access_config=ValkeyAccessConfig(),
        ),
        upload_config=ValkeyUploaderConfig(),
    )

    with pytest.raises(DestinationConnectionError):
        uploader.precheck()


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_with_uri(upload_file: Path, tmp_path: Path):
    """Test connection via URI."""
    key_prefix = "test:uri:"
    index_name = "test_uri_index"
    uri = f"valkey://{VALKEY_TEST_HOST}:{VALKEY_TEST_PORT}"

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                access_config=ValkeyAccessConfig(uri=uri),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix=key_prefix,
                index_name=index_name,
            ),
        )

        file_data = FileData(
            source_identifiers=SourceIdentifiers(
                fullpath=upload_file.name, filename=upload_file.name
            ),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock-file-data",
        )

        with upload_file.open() as f:
            elements = json.load(f)

        await uploader.run_data_async(data=elements, file_data=file_data)
        await validate_upload(elements[0], key_prefix)
        return elements

    try:
        elements = _run_async(run())
    finally:
        keys = [f"{key_prefix}{e['element_id']}" for e in json.loads(upload_file.read_text())]
        _cleanup(keys, index_name)


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_without_embeddings(upload_file: Path, tmp_path: Path):
    """Test uploading elements without embeddings (text/metadata only, no index created)."""
    key_prefix = "test:noemb:"

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix=key_prefix,
                index_name="test_noemb_index",
            ),
        )

        file_data = FileData(
            source_identifiers=SourceIdentifiers(
                fullpath=upload_file.name, filename=upload_file.name
            ),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock-file-data",
        )

        with upload_file.open() as f:
            elements = json.load(f)

        # Strip embeddings from all elements
        for e in elements:
            e.pop("embeddings", None)

        await uploader.run_data_async(data=elements, file_data=file_data)

        # Validate first element was stored (text + metadata, no embedding field)
        client = await get_test_client()
        try:
            key = f"{key_prefix}{elements[0]['element_id']}"
            result = await client.hgetall(key)
            assert result is not None
            assert len(result) > 0

            decoded = {k.decode(): v for k, v in result.items()}
            assert decoded["text"].decode() == elements[0].get("text", "")
            assert decoded["element_type"].decode() == elements[0].get("type", "")
            # No embedding field should be present
            assert "embedding" not in decoded
        finally:
            await client.close()

        return elements

    try:
        elements = _run_async(run())
    finally:
        keys = [f"{key_prefix}{e['element_id']}" for e in json.loads(upload_file.read_text())]
        # Cleanup (no index created for no-embedding path)
        _run_async(cleanup_keys(keys))


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_idempotent(upload_file: Path, tmp_path: Path):
    """Test that uploading the same data twice does not create duplicates."""
    key_prefix = "test:idempotent:"
    index_name = "test_idempotent_index"

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix=key_prefix,
                index_name=index_name,
            ),
        )

        file_data = FileData(
            source_identifiers=SourceIdentifiers(
                fullpath=upload_file.name, filename=upload_file.name
            ),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock-file-data",
        )

        with upload_file.open() as f:
            elements = json.load(f)

        # Upload once
        await uploader.run_data_async(data=elements, file_data=file_data)

        # Upload again (same data)
        await uploader.run_data_async(data=elements, file_data=file_data)

        # Verify no duplicates: count keys matching the prefix
        client = await get_test_client()
        try:
            # Each element should have exactly one key (overwritten, not duplicated)
            for element in elements:
                key = f"{key_prefix}{element['element_id']}"
                result = await client.hgetall(key)
                assert result is not None
                assert len(result) > 0

            # Scan for all keys with the prefix to verify count
            cursor = b"0"
            all_keys = []
            while True:
                cursor, keys = await client.scan(cursor, match=f"{key_prefix}*", count=100)
                all_keys.extend(keys)
                if cursor == b"0":
                    break

            assert len(all_keys) == len(elements), (
                f"Expected {len(elements)} keys, got {len(all_keys)} "
                f"(duplicates exist after re-upload)"
            )
        finally:
            await client.close()

        return elements

    try:
        elements = _run_async(run())
    finally:
        keys = [f"{key_prefix}{e['element_id']}" for e in json.loads(upload_file.read_text())]
        _cleanup(keys, index_name)


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_incremental_upload(upload_file: Path, tmp_path: Path):
    """Test uploading different data to the same index (incremental ingestion)."""
    key_prefix = "test:incremental:"
    index_name = "test_incremental_index"

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix=key_prefix,
                index_name=index_name,
            ),
        )

        file_data = FileData(
            source_identifiers=SourceIdentifiers(
                fullpath=upload_file.name, filename=upload_file.name
            ),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock-file-data",
        )

        with upload_file.open() as f:
            elements = json.load(f)

        # First upload: first half of elements
        first_half = elements[: len(elements) // 2]
        file_data_1 = FileData(
            source_identifiers=SourceIdentifiers(
                fullpath=upload_file.name, filename=upload_file.name
            ),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock-file-data-part1",
        )
        await uploader.run_data_async(data=first_half, file_data=file_data_1)

        # Second upload: second half (different source document, same index)
        second_half = elements[len(elements) // 2 :]
        file_data_2 = FileData(
            source_identifiers=SourceIdentifiers(
                fullpath=upload_file.name, filename=upload_file.name
            ),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock-file-data-part2",
        )
        await uploader.run_data_async(data=second_half, file_data=file_data_2)

        # Verify all elements are stored
        client = await get_test_client()
        try:
            for element in elements:
                key = f"{key_prefix}{element['element_id']}"
                result = await client.hgetall(key)
                assert result is not None and len(result) > 0, (
                    f"Element {element['element_id']} not found after incremental upload"
                )

            # Verify total count matches all elements
            cursor = b"0"
            all_keys = []
            while True:
                cursor, keys = await client.scan(cursor, match=f"{key_prefix}*", count=100)
                all_keys.extend(keys)
                if cursor == b"0":
                    break

            assert len(all_keys) == len(elements), (
                f"Expected {len(elements)} keys, got {len(all_keys)}"
            )
        finally:
            await client.close()

        return elements

    try:
        elements = _run_async(run())
    finally:
        keys = [f"{key_prefix}{e['element_id']}" for e in json.loads(upload_file.read_text())]
        _cleanup(keys, index_name)


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_properly_formed_data(tmp_path: Path):
    """Test that properly formed pipeline output uploads successfully."""
    key_prefix = "test:proper:"

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix=key_prefix,
                index_name="test_proper_index",
            ),
        )

        elements = [
            {
                "element_id": "abc123def456",
                "type": "NarrativeText",
                "text": "This is a properly formed document chunk.",
                "metadata": {"page_number": 1, "filename": "test.pdf"},
                "embeddings": [0.1] * 384,
            }
        ]

        file_data = FileData(
            source_identifiers=SourceIdentifiers(fullpath="test.pdf", filename="test.pdf"),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock",
        )

        await uploader.run_data_async(data=elements, file_data=file_data)

        client = await get_test_client()
        try:
            result = await client.hgetall(f"{key_prefix}abc123def456")
            assert result is not None and len(result) > 0
        finally:
            await client.close()

    _run_async(run())
    _cleanup([f"{key_prefix}abc123def456"], "test_proper_index")


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_missing_element_id():
    """Test that missing element_id raises WriteError, not silent corruption."""
    from unstructured_ingest.error import WriteError

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix="test:bad:",
                index_name="test_bad_index",
            ),
        )

        elements = [
            {
                "type": "NarrativeText",
                "text": "This element has no ID.",
                "metadata": {"page_number": 1},
            }
        ]

        file_data = FileData(
            source_identifiers=SourceIdentifiers(fullpath="test.pdf", filename="test.pdf"),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock",
        )

        await uploader.run_data_async(data=elements, file_data=file_data)

    with pytest.raises(WriteError, match="missing 'element_id'"):
        _run_async(run())


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_empty_element_id():
    """Test that empty element_id raises WriteError."""
    from unstructured_ingest.error import WriteError

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix="test:empty:",
                index_name="test_empty_index",
            ),
        )

        elements = [{"element_id": "", "type": "NarrativeText", "text": "Empty ID."}]

        file_data = FileData(
            source_identifiers=SourceIdentifiers(fullpath="test.pdf", filename="test.pdf"),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock",
        )

        await uploader.run_data_async(data=elements, file_data=file_data)

    with pytest.raises(WriteError, match="missing 'element_id'"):
        _run_async(run())


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_invalid_embeddings_type():
    """Test that non-list embeddings raises WriteError."""
    from unstructured_ingest.error import WriteError

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix="test:badtype:",
                index_name="test_badtype_index",
            ),
        )

        elements = [
            {
                "element_id": "bad_emb_1",
                "type": "NarrativeText",
                "text": "Bad embedding type.",
                "embeddings": "not_a_vector",
            }
        ]

        file_data = FileData(
            source_identifiers=SourceIdentifiers(fullpath="test.pdf", filename="test.pdf"),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock",
        )

        await uploader.run_data_async(data=elements, file_data=file_data)

    with pytest.raises(WriteError, match="invalid 'embeddings' type"):
        _run_async(run())


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_empty_data():
    """Test that empty data list does not crash."""

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix="test:empty_data:",
                index_name="test_empty_data_index",
            ),
        )

        file_data = FileData(
            source_identifiers=SourceIdentifiers(fullpath="test.pdf", filename="test.pdf"),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock",
        )

        await uploader.run_data_async(data=[], file_data=file_data)

    # Should complete without error
    _run_async(run())


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_client_closes_on_exception():
    """Test that client connection is closed even when an exception occurs mid-operation."""

    async def run():
        config = ValkeyConnectionConfig(
            host=VALKEY_TEST_HOST,
            port=VALKEY_TEST_PORT,
            ssl=False,
            access_config=ValkeyAccessConfig(),
        )

        async with config.create_async_client() as client:
            await client.ping()
            raise RuntimeError("simulated failure")

    with pytest.raises(RuntimeError, match="simulated failure"):
        _run_async(run())

    # Verify we can still connect cleanly after the forced error
    async def verify():
        config = ValkeyConnectionConfig(
            host=VALKEY_TEST_HOST,
            port=VALKEY_TEST_PORT,
            ssl=False,
            access_config=ValkeyAccessConfig(),
        )
        async with config.create_async_client() as client:
            result = await client.ping()
            assert result == b"PONG" or result == "PONG"

    _run_async(verify())


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_ttl_individual_path(upload_file: Path, tmp_path: Path):
    """Test TTL on individual write path (when index already exists)."""
    key_prefix = "test:ttl_ind:"
    index_name = "test_ttl_ind_index"

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix=key_prefix,
                index_name=index_name,
                ttl_seconds=3600,
            ),
        )

        file_data = FileData(
            source_identifiers=SourceIdentifiers(
                fullpath=upload_file.name, filename=upload_file.name
            ),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock",
        )

        with upload_file.open() as f:
            elements = json.load(f)

        # First upload creates index via batch path
        await uploader.run_data_async(data=elements, file_data=file_data)

        # Second upload hits individual path (index now exists)
        await uploader.run_data_async(data=elements, file_data=file_data)

        # Verify TTL on keys written via individual path
        client = await get_test_client()
        try:
            key = f"{key_prefix}{elements[0]['element_id']}"
            ttl = await client.ttl(key)
            assert ttl > 0, f"Expected TTL > 0 on individual path, got {ttl}"
        finally:
            await client.close()

        return elements

    elements = _run_async(run())
    _cleanup([f"{key_prefix}{e['element_id']}" for e in elements], index_name)


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
@pytest.mark.parametrize(
    "exc_class,expected_type_name",
    [
        ("TimeoutError", "TimeoutError"),
        ("ConnectionError", "DestinationConnectionError"),
        ("ClosingError", "DestinationConnectionError"),
        ("RequestError", "WriteError"),
    ],
)
def test_map_glide_error(exc_class, expected_type_name):
    """Test that _map_glide_error correctly maps each GLIDE exception type."""
    import glide

    from unstructured_ingest.error import DestinationConnectionError, WriteError
    from unstructured_ingest.error import TimeoutError as IngestTimeoutError

    exc = getattr(glide, exc_class)("test error")
    result = ValkeyUploader._map_glide_error(exc)

    type_map = {
        "TimeoutError": IngestTimeoutError,
        "DestinationConnectionError": DestinationConnectionError,
        "WriteError": WriteError,
    }
    assert isinstance(result, type_map[expected_type_name])


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_map_glide_error_auth():
    """Test that RequestError with auth keywords maps to UserAuthError."""
    import glide

    from unstructured_ingest.error import UserAuthError

    for msg in ["NOAUTH Authentication required", "WRONGPASS invalid password", "NOPERM no permission"]:
        exc = glide.RequestError(msg)
        result = ValkeyUploader._map_glide_error(exc)
        assert isinstance(result, UserAuthError), f"Failed for message: {msg}"


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_reingestion_deletes_stale(tmp_path: Path):
    """Test that re-ingesting a document with different chunks deletes stale keys."""
    key_prefix = "test:reingest:"
    index_name = "test_reingest_index"

    # Clean any leftover state from previous test runs
    _cleanup(
        [f"{key_prefix}{eid}" for eid in ["chunk_a", "chunk_b", "chunk_c", "chunk_x", "chunk_y"]],
        index_name,
    )

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix=key_prefix,
                index_name=index_name,
            ),
        )

        file_data = FileData(
            source_identifiers=SourceIdentifiers(fullpath="doc.pdf", filename="doc.pdf"),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="test-record-123",
        )

        # First upload: 3 elements
        elements_v1 = [
            {"element_id": "chunk_a", "type": "Text", "text": "hello", "embeddings": [0.1] * 384},
            {"element_id": "chunk_b", "type": "Text", "text": "world", "embeddings": [0.2] * 384},
            {"element_id": "chunk_c", "type": "Text", "text": "foo", "embeddings": [0.3] * 384},
        ]
        await uploader.run_data_async(data=elements_v1, file_data=file_data)

        # Allow index backfill to complete before second upload triggers delete-by-record-id
        import asyncio
        await asyncio.sleep(1)

        # Second upload: 2 DIFFERENT elements (simulates re-chunking)
        elements_v2 = [
            {"element_id": "chunk_x", "type": "Text", "text": "new", "embeddings": [0.4] * 384},
            {"element_id": "chunk_y", "type": "Text", "text": "data", "embeddings": [0.5] * 384},
        ]
        await uploader.run_data_async(data=elements_v2, file_data=file_data)

        # Verify: old chunks (a, b, c) should be GONE, new chunks (x, y) should exist
        client = await get_test_client()
        try:
            for old_id in ["chunk_a", "chunk_b", "chunk_c"]:
                result = await client.hgetall(f"{key_prefix}{old_id}")
                assert result is None or len(result) == 0, (
                    f"Stale key {old_id} still exists after re-ingestion"
                )
            for new_id in ["chunk_x", "chunk_y"]:
                result = await client.hgetall(f"{key_prefix}{new_id}")
                assert result is not None and len(result) > 0, (
                    f"New key {new_id} missing after re-ingestion"
                )
        finally:
            await client.close()

        return elements_v2

    elements = _run_async(run())
    _cleanup([f"{key_prefix}{e['element_id']}" for e in elements], index_name)


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_uri_scheme_validation():
    """Test that invalid URI schemes are rejected when connecting."""
    from unstructured_ingest.error import DestinationConnectionError

    config = ValkeyConnectionConfig(
        access_config=ValkeyAccessConfig(uri="vakeys://myhost:6379"),
    )
    uploader = ValkeyUploader(
        connection_config=config,
        upload_config=ValkeyUploaderConfig(),
    )
    with pytest.raises(DestinationConnectionError):
        uploader.precheck()


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_uri_valid_schemes():
    """Test that all valid URI schemes are accepted during param resolution."""
    for scheme in ["valkey", "valkeys", "redis", "rediss"]:
        config = ValkeyConnectionConfig(
            access_config=ValkeyAccessConfig(uri=f"{scheme}://myhost:6379"),
        )
        # Should not raise during param resolution
        host, port, password, username, use_tls = config._resolve_connection_params()
        assert host == "myhost"
        assert port == 6379
        if scheme in ("valkeys", "rediss"):
            assert use_tls is True
        else:
            assert use_tls is False


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_distance_metric_validation():
    """Test that invalid distance_metric values are rejected by pydantic."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ValkeyUploaderConfig(distance_metric="EUCLIDEAN")


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_distance_metric_valid():
    """Test that valid distance_metric values are accepted."""
    for metric in ["COSINE", "L2", "IP"]:
        config = ValkeyUploaderConfig(distance_metric=metric)
        assert config.distance_metric == metric


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_atomic_hset_expire(upload_file: Path, tmp_path: Path):
    """Test that TTL is applied atomically via individual write path (index active).

    Verifies that the atomic Batch(is_atomic=True) for HSET+EXPIRE works correctly
    on the individual write path (when index already exists).
    """
    key_prefix = "test:atomic_ttl:"
    index_name = "test_atomic_ttl_index"

    # Clean any leftover state
    _cleanup(
        [f"{key_prefix}abc_atomic"],
        index_name,
    )

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix=key_prefix,
                index_name=index_name,
                ttl_seconds=7200,
            ),
        )

        file_data = FileData(
            source_identifiers=SourceIdentifiers(fullpath="test.pdf", filename="test.pdf"),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="atomic-test-record",
        )

        elements = [
            {"element_id": "abc_atomic", "type": "Text", "text": "atomic test", "embeddings": [0.5] * 384},
        ]

        # First upload: creates index via batch path
        await uploader.run_data_async(data=elements, file_data=file_data)

        # Second upload: hits individual path (index exists), uses atomic HSET+EXPIRE
        await uploader.run_data_async(data=elements, file_data=file_data)

        # Verify TTL was applied atomically
        client = await get_test_client()
        try:
            key = f"{key_prefix}abc_atomic"
            ttl = await client.ttl(key)
            assert ttl > 0, f"Expected TTL > 0 (atomic HSET+EXPIRE), got {ttl}"
            # TTL should be close to 7200 (just written)
            assert ttl > 7000, f"TTL {ttl} is too low — expected ~7200"
        finally:
            await client.close()

    _run_async(run())
    _cleanup([f"{key_prefix}abc_atomic"], index_name)


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_reingestion_with_path_identifier(tmp_path: Path):
    """Regression: identifiers with dots/slashes must be escaped in FT.SEARCH tag queries."""
    key_prefix = "test:pathid:"
    index_name = "test_pathid_index"

    # Clean any leftover state
    _cleanup(
        [f"{key_prefix}{eid}" for eid in ["chunk_old", "chunk_new"]],
        index_name,
    )

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix=key_prefix,
                index_name=index_name,
            ),
        )

        # Realistic file path identifier with dots and slashes
        file_data = FileData(
            source_identifiers=SourceIdentifiers(
                fullpath="s3://bucket/reports/quarterly.pdf",
                filename="quarterly.pdf",
            ),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="s3://bucket/reports/quarterly.pdf",
        )

        # First upload
        elements_v1 = [
            {"element_id": "chunk_old", "type": "Text", "text": "old data", "embeddings": [0.1] * 384},
        ]
        await uploader.run_data_async(data=elements_v1, file_data=file_data)

        await asyncio.sleep(1)  # Allow index backfill

        # Second upload with different chunks (re-ingestion)
        elements_v2 = [
            {"element_id": "chunk_new", "type": "Text", "text": "new data", "embeddings": [0.2] * 384},
        ]
        await uploader.run_data_async(data=elements_v2, file_data=file_data)

        # Old chunk should be deleted, new chunk should exist
        client = await get_test_client()
        try:
            old_result = await client.hgetall(f"{key_prefix}chunk_old")
            assert old_result is None or len(old_result) == 0, (
                "Stale key 'chunk_old' still exists — tag escaping for dots/slashes is broken"
            )
            new_result = await client.hgetall(f"{key_prefix}chunk_new")
            assert new_result is not None and len(new_result) > 0, (
                "New key 'chunk_new' missing after re-ingestion"
            )
        finally:
            await client.close()

        return elements_v2

    elements = _run_async(run())
    _cleanup([f"{key_prefix}{e['element_id']}" for e in elements], index_name)


@pytest.mark.tags(VALKEY_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_valkey_destination_vector_upload_fails_without_search_module():
    """Test that uploading elements with embeddings raises DestinationConnectionError
    when the ValkeySearch module is not available (simulated via patching _check_index)."""
    from unittest.mock import AsyncMock, patch

    from unstructured_ingest.error import DestinationConnectionError

    async def run():
        uploader = ValkeyUploader(
            connection_config=ValkeyConnectionConfig(
                host=VALKEY_TEST_HOST,
                port=VALKEY_TEST_PORT,
                ssl=False,
                access_config=ValkeyAccessConfig(),
            ),
            upload_config=ValkeyUploaderConfig(
                batch_size=10,
                key_prefix="test:no_module:",
                index_name="test_no_module_index",
            ),
        )

        file_data = FileData(
            source_identifiers=SourceIdentifiers(fullpath="test.pdf", filename="test.pdf"),
            connector_type=VALKEY_CONNECTOR_TYPE,
            identifier="mock-no-module",
        )

        elements = [
            {
                "element_id": "vec_no_mod_1",
                "type": "NarrativeText",
                "text": "This has embeddings but no search module.",
                "embeddings": [0.1] * 384,
            }
        ]

        # Patch _check_index to simulate ValkeySearch module not loaded
        with patch.object(
            ValkeyUploader, "_check_index", new_callable=AsyncMock, return_value=(False, False)
        ):
            await uploader.run_data_async(data=elements, file_data=file_data)

    with pytest.raises(DestinationConnectionError, match="Search module not loaded"):
        _run_async(run())
