import random

import faker
import pytest

from unstructured_ingest.embed.bedrock import BedrockEmbeddingConfig, BedrockEmbeddingEncoder

fake = faker.Faker()


def generate_embedder_config_params() -> dict:
    params = {
        "aws_access_key_id": fake.password(),
        "aws_secret_access_key": fake.password(),
        "region_name": fake.city(),
    }
    if random.random() < 0.5:
        params["embed_model_name"] = fake.word()
    return params


@pytest.mark.parametrize(
    "embedder_config_params", [generate_embedder_config_params() for i in range(10)]
)
def test_embedder_config(embedder_config_params: dict):
    embedder_config = BedrockEmbeddingConfig.model_validate(embedder_config_params)
    assert embedder_config


@pytest.mark.parametrize(
    "embedder_config_params", [generate_embedder_config_params() for i in range(10)]
)
def test_embedder(embedder_config_params: dict):
    embedder_config = BedrockEmbeddingConfig.model_validate(embedder_config_params)
    embedder = BedrockEmbeddingEncoder(config=embedder_config)
    assert embedder
