import json
import os
from pathlib import Path

import pytest

from test.integration.embedders.utils import (
    validate_embedding_output,
    validate_raw_embedder,
    validate_raw_embedder_async,
)
from test.integration.utils import requires_env
from unstructured_ingest.embed.octoai import (
    AsyncOctoAIEmbeddingEncoder,
    OctoAiEmbeddingConfig,
    OctoAIEmbeddingEncoder,
)
from unstructured_ingest.v2.errors import UserAuthError
from unstructured_ingest.v2.processes.embedder import Embedder, EmbedderConfig

API_KEY = "OCTOAI_API_KEY"


def get_api_key() -> str:
    api_key = os.getenv(API_KEY, None)
    assert api_key
    return api_key


@requires_env(API_KEY)
def test_octoai_embedder(embedder_file: Path):
    api_key = get_api_key()
    embedder_config = EmbedderConfig(embedding_provider="octoai", embedding_api_key=api_key)
    embedder = Embedder(config=embedder_config)
    results = embedder.run(elements_filepath=embedder_file)
    assert results
    with embedder_file.open("r") as f:
        original_elements = json.load(f)
    validate_embedding_output(original_elements=original_elements, output_elements=results)


@requires_env(API_KEY)
def test_raw_octoai_embedder(embedder_file: Path):
    api_key = get_api_key()
    embedder = OctoAIEmbeddingEncoder(
        config=OctoAiEmbeddingConfig(
            api_key=api_key,
        )
    )
    validate_raw_embedder(embedder=embedder, embedder_file=embedder_file, expected_dimension=1024)


@pytest.mark.skip(reason="Unexpected connection error at the moment")
def test_raw_octoai_embedder_invalid_credentials():
    embedder = OctoAIEmbeddingEncoder(
        config=OctoAiEmbeddingConfig(
            api_key="fake_api_key",
        )
    )
    with pytest.raises(UserAuthError):
        embedder.get_exemplary_embedding()


@requires_env(API_KEY)
@pytest.mark.asyncio
async def test_raw_async_octoai_embedder(embedder_file: Path):
    api_key = get_api_key()
    embedder = AsyncOctoAIEmbeddingEncoder(
        config=OctoAiEmbeddingConfig(
            api_key=api_key,
        )
    )
    await validate_raw_embedder_async(
        embedder=embedder, embedder_file=embedder_file, expected_dimension=1024
    )
