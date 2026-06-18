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

    elements = _run_async(run())
    keys = [f"{key_prefix}{e['element_id']}" for e in elements]

    # Cleanup
    async def cleanup():
        await cleanup_keys(keys)
        try:
            client = await get_test_client()
            from glide import ft

            await ft.dropindex(client, index_name)
            await client.close()
        except Exception:
            pass

    _run_async(cleanup())


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

    elements = _run_async(run())
    keys = [f"{key_prefix}{e['element_id']}" for e in elements]

    # Cleanup
    async def cleanup():
        await cleanup_keys(keys)
        try:
            client = await get_test_client()
            from glide import ft

            await ft.dropindex(client, index_name)
            await client.close()
        except Exception:
            pass

    _run_async(cleanup())


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

    elements = _run_async(run())
    keys = [f"{key_prefix}{e['element_id']}" for e in elements]

    # Cleanup
    async def cleanup():
        await cleanup_keys(keys)
        try:
            client = await get_test_client()
            from glide import ft

            await ft.dropindex(client, index_name)
            await client.close()
        except Exception:
            pass

    _run_async(cleanup())


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

    elements = _run_async(run())
    keys = [f"{key_prefix}{e['element_id']}" for e in elements]

    # Cleanup
    async def cleanup():
        await cleanup_keys(keys)

    _run_async(cleanup())


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

    elements = _run_async(run())
    keys = [f"{key_prefix}{e['element_id']}" for e in elements]

    # Cleanup
    async def cleanup():
        await cleanup_keys(keys)
        try:
            client = await get_test_client()
            from glide import ft

            await ft.dropindex(client, index_name)
            await client.close()
        except Exception:
            pass

    _run_async(cleanup())


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
        await uploader.run_data_async(data=first_half, file_data=file_data)

        # Second upload: second half (different data, same index)
        second_half = elements[len(elements) // 2 :]
        await uploader.run_data_async(data=second_half, file_data=file_data)

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

    elements = _run_async(run())
    keys = [f"{key_prefix}{e['element_id']}" for e in elements]

    # Cleanup
    async def cleanup():
        await cleanup_keys(keys)
        try:
            client = await get_test_client()
            from glide import ft

            await ft.dropindex(client, index_name)
            await client.close()
        except Exception:
            pass

    _run_async(cleanup())


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

    async def cleanup():
        await cleanup_keys([f"{key_prefix}abc123def456"])
        try:
            client = await get_test_client()
            from glide import ft

            await ft.dropindex(client, "test_proper_index")
            await client.close()
        except Exception:
            pass

    _run_async(cleanup())


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
