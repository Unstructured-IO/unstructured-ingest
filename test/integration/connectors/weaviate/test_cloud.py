import pytest
from pydantic import ValidationError

from unstructured_ingest.v2.processes.connectors.weaviate.cloud import (
    CloudWeaviateAccessConfig,
    CloudWeaviateConnectionConfig,
)


def test_weaviate_failing_connection_config():
    with pytest.raises(ValidationError):
        CloudWeaviateConnectionConfig(
            access_config=CloudWeaviateAccessConfig(api_key="my key", password="password"),
            username="username",
            cluster_url="clusterurl",
        )


def test_weaviate_connection_config_happy_path():
    CloudWeaviateConnectionConfig(
        access_config=CloudWeaviateAccessConfig(
            api_key="my key",
        ),
        cluster_url="clusterurl",
    )


def test_weaviate_connection_config_anonymous():
    CloudWeaviateConnectionConfig(
        access_config=CloudWeaviateAccessConfig(api_key="my key", password="password"),
        username="username",
        anonymous=True,
        cluster_url="clusterurl",
    )
