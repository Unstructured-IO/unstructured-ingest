import os
import random
from unittest.mock import patch

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
        params["embedder_model_name"] = fake.word()
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


def test_inference_profile_default():
    config = BedrockEmbeddingConfig(aws_access_key_id="test", aws_secret_access_key="test")
    assert config.inference_profile_id is None


@patch.dict(os.environ, {"BEDROCK_INFERENCE_PROFILE_ID": "test-profile"})
def test_inference_profile_env_var():
    config = BedrockEmbeddingConfig(aws_access_key_id="test", aws_secret_access_key="test")
    assert config.inference_profile_id == "test-profile"


def test_inference_profile_manual():
    config = BedrockEmbeddingConfig(
        aws_access_key_id="test",
        aws_secret_access_key="test",
        inference_profile_id="manual-profile"
    )
    assert config.inference_profile_id == "manual-profile"
