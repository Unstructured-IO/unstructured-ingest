import random

import faker
import pytest

from unstructured_ingest.embed.mixedbreadai import (
    MixedbreadAIEmbeddingConfig,
    MixedbreadAIEmbeddingEncoder,
)

fake = faker.Faker()


def generate_embedder_config_params() -> dict:
    params = {
        "api_key": fake.password(),
    }
    if random.random() < 0.5:
        params["embedder_model_name"] = fake.word()
    return params


@pytest.mark.parametrize(
    "embedder_config_params", [generate_embedder_config_params() for i in range(10)]
)
def test_embedder_config(embedder_config_params: dict):
    embedder_config = MixedbreadAIEmbeddingConfig.model_validate(embedder_config_params)
    assert embedder_config


@pytest.mark.parametrize(
    "embedder_config_params", [generate_embedder_config_params() for i in range(10)]
)
def test_embedder(embedder_config_params: dict):
    embedder_config = MixedbreadAIEmbeddingConfig.model_validate(embedder_config_params)
    embedder = MixedbreadAIEmbeddingEncoder(config=embedder_config)
    assert embedder
