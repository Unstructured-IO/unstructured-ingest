import pytest

from test.integration.connectors.utils.constants import DESTINATION_TAG, VECTOR_DB_TAG
from unstructured_ingest.errors_v2 import ValueError
from unstructured_ingest.processes.connectors.weaviate.cloud import (
    CONNECTOR_TYPE,
    CloudWeaviateAccessConfig,
    CloudWeaviateConnectionConfig,
)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_weaviate_failing_connection_config():
    with pytest.raises(ValueError):
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
