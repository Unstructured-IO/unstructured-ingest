from unittest.mock import MagicMock, patch

import pytest
from pydantic import Secret

from unstructured_ingest.__version__ import __version__
from unstructured_ingest.processes.connectors.weaviate.cloud import (
    CloudWeaviateAccessConfig,
    CloudWeaviateConnectionConfig,
)
from unstructured_ingest.processes.connectors.weaviate.embedded import (
    EmbeddedWeaviateConnectionConfig,
)
from unstructured_ingest.processes.connectors.weaviate.local import (
    LocalWeaviateConnectionConfig,
)
from unstructured_ingest.processes.connectors.weaviate.weaviate import INTEGRATION_HEADER

EXPECTED_HEADERS = {INTEGRATION_HEADER: f"unstructured-ingest/{__version__}"}


@pytest.mark.parametrize(
    ("factory", "connection_config"),
    [
        ("connect_to_local", LocalWeaviateConnectionConfig()),
        ("connect_to_embedded", EmbeddedWeaviateConnectionConfig()),
        (
            "connect_to_weaviate_cloud",
            CloudWeaviateConnectionConfig(
                cluster_url="https://example.weaviate.cloud",
                access_config=Secret(CloudWeaviateAccessConfig(api_key="key")),
            ),
        ),
    ],
)
def test_get_client_sends_integration_header(factory: str, connection_config):
    with (
        patch(f"weaviate.{factory}", return_value=MagicMock()) as connect,
        connection_config.get_client(),
    ):
        pass

    assert connect.call_args.kwargs["headers"] == EXPECTED_HEADERS
