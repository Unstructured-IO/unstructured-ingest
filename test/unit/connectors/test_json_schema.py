import pytest

from unstructured_ingest.processes.connector_registry import (
    destination_registry,
    source_registry,
)


def test_all_source_connector_configs_json_schema():
    failed_connectors = []

    for connector_type, connector_entry in source_registry.items():
        try:
            if connector_entry.connection_config:
                schema = connector_entry.connection_config.model_json_schema()
                assert schema is not None, f"Schema is None for {connector_type} connection_config"

            if connector_entry.indexer_config:
                schema = connector_entry.indexer_config.model_json_schema()
                assert schema is not None, f"Schema is None for {connector_type} indexer_config"

            if connector_entry.downloader_config:
                schema = connector_entry.downloader_config.model_json_schema()
                assert schema is not None, f"Schema is None for {connector_type} downloader_config"

        except Exception as e:
            failed_connectors.append((connector_type, str(e)))

    if failed_connectors:
        error_msg = "Failed to generate JSON schema for source connectors:\n"
        for connector_type, error in failed_connectors:
            error_msg += f"  - {connector_type}: {error}\n"
        pytest.fail(error_msg)


def test_all_destination_connector_configs_json_schema():
    failed_connectors = []

    for connector_type, connector_entry in destination_registry.items():
        try:
            if connector_entry.connection_config:
                schema = connector_entry.connection_config.model_json_schema()
                assert schema is not None, f"Schema is None for {connector_type} connection_config"

            if connector_entry.uploader_config:
                schema = connector_entry.uploader_config.model_json_schema()
                assert schema is not None, f"Schema is None for {connector_type} uploader_config"

            if connector_entry.upload_stager_config:
                schema = connector_entry.upload_stager_config.model_json_schema()
                assert schema is not None, (
                    f"Schema is None for {connector_type} upload_stager_config"
                )

        except Exception as e:
            failed_connectors.append((connector_type, str(e)))

    if failed_connectors:
        error_msg = "Failed to generate JSON schema for destination connectors:\n"
        for connector_type, error in failed_connectors:
            error_msg += f"  - {connector_type}: {error}\n"
        pytest.fail(error_msg)
