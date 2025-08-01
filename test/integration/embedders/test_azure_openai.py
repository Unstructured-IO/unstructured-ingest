import json
import os
from dataclasses import dataclass
from pathlib import Path

import pydantic
import pytest

from test.integration.embedders.utils import validate_embedding_output, validate_raw_embedder
from test.integration.utils import requires_env
from unstructured_ingest.embed.azure_openai import (
    AzureOpenAIEmbeddingConfig,
    AzureOpenAIEmbeddingEncoder,
)
from unstructured_ingest.processes.embedder import Embedder, EmbedderConfig

API_KEY = "AZURE_OPENAI_API_KEY"
ENDPOINT = "AZURE_OPENAI_ENDPOINT"


@dataclass(frozen=True)
class AzureData:
    api_key: str
    endpoint: str


def get_azure_data() -> AzureData:
    api_key = os.getenv(API_KEY, None)
    assert api_key
    endpoint = os.getenv(ENDPOINT, None)
    assert endpoint
    return AzureData(api_key, endpoint)


@requires_env(API_KEY, ENDPOINT)
def test_azure_openai_embedder(embedder_file: Path):
    azure_data = get_azure_data()
    embedder_config = EmbedderConfig(
        embedding_provider="azure-openai",
        embedding_api_key=azure_data.api_key,
        embedding_azure_endpoint=azure_data.endpoint,
    )
    embedder = Embedder(config=embedder_config)
    embedder.precheck()
    results = embedder.run(elements_filepath=embedder_file)
    assert results
    with embedder_file.open("r") as f:
        original_elements = json.load(f)
    validate_embedding_output(original_elements=original_elements, output_elements=results)


@requires_env(API_KEY, ENDPOINT)
def test_raw_azure_openai_embedder(embedder_file: Path):
    azure_data = get_azure_data()
    embedder = AzureOpenAIEmbeddingEncoder(
        config=AzureOpenAIEmbeddingConfig(
            api_key=azure_data.api_key,
            azure_endpoint=azure_data.endpoint,
        )
    )
    embedder.precheck()
    validate_raw_embedder(embedder=embedder, embedder_file=embedder_file, expected_dimension=1536)


def test_openai_custom_tls_no_override_should_fail(mock_embeddings_server, embedder_file: Path):
    from openai import APIConnectionError

    port, certificate_path, counter = mock_embeddings_server
    calls_before = counter["POST"]
    with pytest.raises(APIConnectionError):
        embedder_config = EmbedderConfig(
            embedding_provider="azure-openai",
            embedding_api_key=pydantic.SecretStr("foo"),
            embedding_azure_endpoint=f"https://localhost:{port}",
        )
        embedder = Embedder(config=embedder_config)
        embedder.precheck()
        _ = embedder.run(elements_filepath=embedder_file)

    assert counter["POST"] == calls_before, (
        f"Expected to see no change to POST calls toward embedder, got {counter} != {calls_before}"
    )


def test_openai_custom_tls_with_override_should_succeed(
    mock_embeddings_server, monkeypatch, embedder_file: Path
):
    port, certificate_path, counter = mock_embeddings_server
    calls_before = counter["POST"]
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", certificate_path)
    embedder_config = EmbedderConfig(
        embedding_provider="azure-openai",
        embedding_api_key=pydantic.SecretStr("foo"),
        embedding_azure_endpoint=f"https://localhost:{port}",
    )
    embedder = Embedder(config=embedder_config)
    embedder.precheck()
    results = embedder.run(elements_filepath=embedder_file)
    assert results
    assert counter["POST"] > calls_before, (
        f"Expected to see more POST calls to embedder, got {counter} from {calls_before}"
    )
