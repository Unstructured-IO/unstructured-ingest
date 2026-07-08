import pytest

# Importing the connectors package registers every entry via import-time side effects.
import unstructured_ingest.processes.connectors  # noqa: F401
from unstructured_ingest.processes.connector_registry import (
    LocationShape,
    SourceRegistryEntry,
    destination_registry,
    source_registry,
)

# fsspec sources that emit a per-record version (sftp does not).
VERSION_EMITTING_FSSPEC = ("s3", "azure", "gcs", "box", "dropbox")
FSSPEC_COHORT = VERSION_EMITTING_FSSPEC + ("sftp",)


@pytest.mark.parametrize("connector_type", FSSPEC_COHORT)
def test_fsspec_source_entry_markers(connector_type):
    entry = source_registry[connector_type]
    assert entry.location_shape == LocationShape.FSSPEC_URL
    assert entry.location_identity == ("indexer_config.remote_url",)
    assert entry.emits_record_version == (connector_type in VERSION_EMITTING_FSSPEC)

    schema = entry.indexer_config.model_json_schema()
    assert schema["properties"]["remote_url"].get("x-runtime-eligible") is True
    assert schema["properties"]["recursive"].get("x-runtime-eligible") is True


@pytest.mark.parametrize("connector_type", FSSPEC_COHORT)
def test_fsspec_destination_entry_markers(connector_type):
    entry = destination_registry[connector_type]
    assert entry.location_shape == LocationShape.FSSPEC_URL
    assert entry.location_identity == ("uploader_config.remote_url",)

    schema = entry.uploader_config.model_json_schema()
    assert schema["properties"]["remote_url"].get("x-runtime-eligible") is True


def test_sql_table_source_entry_markers():
    # postgres represents the sql-table cohort: connection database + indexer table
    # compose the equality identity, no recursion, no record version.
    entry = source_registry["postgres"]
    assert entry.location_shape == LocationShape.SQL_TABLE
    assert entry.location_identity == (
        "connector_config.database",
        "indexer_config.table_name",
    )
    assert entry.supports_recursion is False
    assert entry.emits_record_version is False

    conn = entry.connection_config.model_json_schema()
    assert conn["properties"]["database"].get("x-runtime-eligible") is True
    idx = entry.indexer_config.model_json_schema()
    assert idx["properties"]["table_name"].get("x-runtime-eligible") is True


def test_sql_table_destination_entry_markers():
    # kdbai represents the destination-only sql-table cohort.
    entry = destination_registry["kdbai"]
    assert entry.location_shape == LocationShape.SQL_TABLE
    assert entry.location_identity == (
        "uploader_config.database_name",
        "uploader_config.table_name",
    )
    assert entry.supports_recursion is False

    up = entry.uploader_config.model_json_schema()
    assert up["properties"]["database_name"].get("x-runtime-eligible") is True
    assert up["properties"]["table_name"].get("x-runtime-eligible") is True


def test_search_index_destination_entry_markers():
    # pinecone represents the search-index cohort: a connection-hosted index name,
    # equality identity, no recursion.
    entry = destination_registry["pinecone"]
    assert entry.location_shape == LocationShape.SEARCH_INDEX
    assert entry.location_identity == ("connector_config.index_name",)
    assert entry.supports_recursion is False

    conn = entry.connection_config.model_json_schema()
    assert conn["properties"]["index_name"].get("x-runtime-eligible") is True


def test_search_index_uploader_collection_marker():
    # weaviate-cloud keeps its collection on the uploader config; only that leaf is
    # runtime-eligible (the cluster_url is identity-only).
    entry = destination_registry["weaviate-cloud"]
    assert entry.location_shape == LocationShape.SEARCH_INDEX
    assert entry.location_identity == (
        "connector_config.cluster_url",
        "uploader_config.collection",
    )
    up = entry.uploader_config.model_json_schema()
    assert up["properties"]["collection"].get("x-runtime-eligible") is True


def test_unannotated_entry_is_unmarked():
    # A connector that sets no markers reports location_shape None so consumers
    # fall back to their own defaults rather than deriving an fsspec identity.
    entry = SourceRegistryEntry(indexer=object, downloader=object)
    assert entry.location_shape is None
    assert entry.emits_record_version is False
