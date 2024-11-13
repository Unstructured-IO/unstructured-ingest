import os

import pytest

from test.integration.connectors.utils.constants import DESTINATION_TAG, SOURCE_TAG
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.connectors.astradb import (
    CONNECTOR_TYPE,
    AstraDBAccessConfig,
    AstraDBConnectionConfig,
    AstraDBIndexer,
    AstraDBIndexerConfig,
    AstraDBUploader,
    AstraDBUploaderConfig,
    DestinationConnectionError,
    SourceConnectionError,
)

EXISTENT_COLLECTION_NAME = "ingest_test_src"
NONEXISTENT_COLLECTION_NAME = "nonexistant"


@pytest.fixture
def connection_config() -> AstraDBConnectionConfig:
    return AstraDBConnectionConfig(
        access_config=AstraDBAccessConfig(
            token=os.environ["ASTRA_DB_APPLICATION_TOKEN"],
            api_endpoint=os.environ["ASTRA_DB_API_ENDPOINT"],
        )
    )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, DESTINATION_TAG)
@requires_env("ASTRA_DB_APPLICATION_TOKEN", "ASTRA_DB_API_ENDPOINT")
def test_precheck_succeeds(connection_config: AstraDBConnectionConfig):
    indexer = AstraDBIndexer(
        connection_config=connection_config,
        index_config=AstraDBIndexerConfig(collection_name=EXISTENT_COLLECTION_NAME),
    )
    uploader = AstraDBUploader(
        connection_config=connection_config,
        upload_config=AstraDBUploaderConfig(collection_name=EXISTENT_COLLECTION_NAME),
    )
    indexer.precheck()
    uploader.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, DESTINATION_TAG)
@requires_env("ASTRA_DB_APPLICATION_TOKEN", "ASTRA_DB_API_ENDPOINT")
def test_precheck_fails(connection_config: AstraDBConnectionConfig):
    indexer = AstraDBIndexer(
        connection_config=connection_config,
        index_config=AstraDBIndexerConfig(collection_name=NONEXISTENT_COLLECTION_NAME),
    )
    uploader = AstraDBUploader(
        connection_config=connection_config,
        upload_config=AstraDBUploaderConfig(collection_name=NONEXISTENT_COLLECTION_NAME),
    )
    with pytest.raises(expected_exception=SourceConnectionError):
        indexer.precheck()
    with pytest.raises(expected_exception=DestinationConnectionError):
        uploader.precheck()
