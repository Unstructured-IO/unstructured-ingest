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


def test_unannotated_entry_is_unmarked():
    # A connector that sets no markers reports location_shape None so consumers
    # fall back to their own defaults rather than deriving an fsspec identity.
    entry = SourceRegistryEntry(indexer=object, downloader=object)
    assert entry.location_shape is None
    assert entry.emits_record_version is False
