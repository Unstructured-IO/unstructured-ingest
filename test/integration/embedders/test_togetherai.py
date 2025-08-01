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
from unstructured_ingest.embed.togetherai import (
    AsyncTogetherAIEmbeddingEncoder,
    TogetherAIEmbeddingConfig,
    TogetherAIEmbeddingEncoder,
)
from unstructured_ingest.errors_v2 import UserAuthError
from unstructured_ingest.processes.embedder import Embedder, EmbedderConfig

API_KEY = "TOGETHERAI_API_KEY"


def get_api_key() -> str:
    api_key = os.getenv(API_KEY, None)
    assert api_key
    return api_key


# TODO: re-enable these tests when TogetherAI API is fixed


@pytest.mark.skip(reason="TogetherAI API disabled")
@requires_env(API_KEY)
def test_togetherai_embedder(embedder_file: Path):
    api_key = get_api_key()
    embedder_config = EmbedderConfig(embedding_provider="togetherai", embedding_api_key=api_key)
    embedder = Embedder(config=embedder_config)
    embedder.precheck()
    results = embedder.run(elements_filepath=embedder_file)
    assert results
    with embedder_file.open("r") as f:
        original_elements = json.load(f)
    validate_embedding_output(original_elements=original_elements, output_elements=results)


@pytest.mark.skip(reason="TogetherAI API disabled")
@requires_env(API_KEY)
def test_raw_togetherai_embedder(embedder_file: Path):
    api_key = get_api_key()
    embedder = TogetherAIEmbeddingEncoder(config=TogetherAIEmbeddingConfig(api_key=api_key))
    embedder.precheck()
    validate_raw_embedder(
        embedder=embedder,
        embedder_file=embedder_file,
        expected_dimension=768,
        expected_is_unit_vector=False,
    )


@pytest.mark.skip(reason="TogetherAI API disabled")
def test_raw_togetherai_embedder_invalid_credentials():
    embedder = TogetherAIEmbeddingEncoder(config=TogetherAIEmbeddingConfig(api_key="fake_api_key"))

    with pytest.raises(UserAuthError):
        embedder.get_exemplary_embedding()


@pytest.mark.skip(reason="TogetherAI API disabled")
@requires_env(API_KEY)
@pytest.mark.asyncio
async def test_raw_async_togetherai_embedder(embedder_file: Path):
    api_key = get_api_key()
    embedder = AsyncTogetherAIEmbeddingEncoder(config=TogetherAIEmbeddingConfig(api_key=api_key))
    embedder.precheck()
    await validate_raw_embedder_async(
        embedder=embedder,
        embedder_file=embedder_file,
        expected_dimension=768,
        expected_is_unit_vector=False,
    )
