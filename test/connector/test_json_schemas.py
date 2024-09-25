import unstructured_ingest.v2.processes.connectors  # noqa
from unstructured_ingest.v2.processes.connector_registry import (
    destination_registry,
    source_registry,
)


def test_source_registry():
    for entry in source_registry.values():
        if downloader_config := entry.downloader_config:
            assert downloader_config.model_json_schema()
        if indexer_config := entry.indexer_config:
            assert indexer_config.model_json_schema()
        if connection_config := entry.connection_config:
            assert connection_config.model_json_schema()


def test_destination_registry():
    for entry in destination_registry.values():
        if connection_config := entry.connection_config:
            assert connection_config.model_json_schema()
        if uploader_config := entry.uploader_config:
            assert uploader_config.model_json_schema()
        if upload_stager_config := entry.upload_stager_config:
            assert upload_stager_config.model_json_schema()
