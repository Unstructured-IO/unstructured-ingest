import json
import os
from dataclasses import dataclass
from pathlib import Path

from test.integration.embedders.utils import validate_embedding_output, validate_raw_embedder
from test.integration.utils import requires_env
from unstructured_ingest.embed.azure_openai import (
    AzureOpenAIEmbeddingConfig,
    AzureOpenAIEmbeddingEncoder,
)
from unstructured_ingest.v2.processes.embedder import Embedder, EmbedderConfig

API_KEY = "AZURE_OPENAI_API_KEY"
API_VERSION = "AZURE_OPENAI_API_VERSION"
ENDPOINT = "AZURE_OPENAI_ENDPOINT"


@dataclass(frozen=True)
class AzureData:
    api_key: str
    api_version: str
    endpoint: str


def get_azure_data() -> AzureData:
    api_key = os.getenv(API_KEY, None)
    assert api_key
    api_version = os.getenv(API_VERSION, None)
    assert api_version
    endpoint = os.getenv(ENDPOINT, None)
    assert endpoint
    return AzureData(api_key, api_version, endpoint)


@requires_env(API_KEY, API_VERSION, ENDPOINT)
def test_azure_openai_embedder(embedder_file: Path):
    azure_data = get_azure_data()
    embedder_config = EmbedderConfig(
        embedding_provider="openai",
        embedding_api_key=azure_data.api_key,
        embedding_azure_api_version=azure_data.api_version,
        embedding_azure_endpoint=azure_data.endpoint,
    )
    embedder = Embedder(config=embedder_config)
    results = embedder.run(elements_filepath=embedder_file)
    assert results
    with embedder_file.open("r") as f:
        original_elements = json.load(f)
    validate_embedding_output(original_elements=original_elements, output_elements=results)


@requires_env(API_KEY)
def test_raw_azure_openai_embedder(embedder_file: Path):
    azure_data = get_azure_data()
    embedder = AzureOpenAIEmbeddingEncoder(
        config=AzureOpenAIEmbeddingConfig(
            api_key=azure_data.api_key,
            api_version=azure_data.api_version,
            azure_endpoint=azure_data.endpoint,
        )
    )
    validate_raw_embedder(
        embedder=embedder, embedder_file=embedder_file, expected_dimensions=(1536,)
    )
