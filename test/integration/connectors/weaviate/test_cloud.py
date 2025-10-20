import pytest

from test.integration.connectors.utils.constants import DESTINATION_TAG, VECTOR_DB_TAG
from unstructured_ingest.error import (
    DestinationConnectionError,
    ValueError as IngestValueError,
)
from unstructured_ingest.processes.connectors.weaviate.cloud import (
    CONNECTOR_TYPE,
    CloudWeaviateAccessConfig,
    CloudWeaviateConnectionConfig,
    CloudWeaviateUploader,
    CloudWeaviateUploaderConfig,
)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_weaviate_failing_connection_config():
    with pytest.raises(IngestValueError):
        CloudWeaviateConnectionConfig(
            access_config=CloudWeaviateAccessConfig(api_key="my key", password="password"),
            username="username",
            cluster_url="clusterurl",
        )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_weaviate_connection_config_happy_path():
    CloudWeaviateConnectionConfig(
        access_config=CloudWeaviateAccessConfig(
            api_key="my key",
        ),
        cluster_url="clusterurl",
    )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_weaviate_connection_config_anonymous():
    CloudWeaviateConnectionConfig(
        access_config=CloudWeaviateAccessConfig(api_key="my key", password="password"),
        username="username",
        anonymous=True,
        cluster_url="clusterurl",
    )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_weaviate_precheck_invalid_credentials():
    """Test that precheck properly validates connection with invalid credentials."""
    connection_config = CloudWeaviateConnectionConfig(
        access_config=CloudWeaviateAccessConfig(api_key="invalid-test-key-12345"),
        cluster_url="https://invalid-test-cluster.weaviate.cloud",
        anonymous=False,
    )
    upload_config = CloudWeaviateUploaderConfig(collection=None)
    uploader = CloudWeaviateUploader(
        connection_config=connection_config,
        upload_config=upload_config,
    )
    with pytest.raises(DestinationConnectionError):
        uploader.precheck()
