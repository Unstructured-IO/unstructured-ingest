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
        inference_profile_id="manual-profile",
    )
    assert config.inference_profile_id == "manual-profile"


def test_iam_access_method_uses_ambient_credentials():
    config = BedrockEmbeddingConfig(access_method="iam", region_name="us-west-2")

    assert config.get_client_kwargs() == {"region_name": "us-west-2"}


def test_missing_access_method_without_fields_uses_ambient_credentials():
    config = BedrockEmbeddingConfig(region_name="us-west-2")

    assert config.get_client_kwargs() == {"region_name": "us-west-2"}


def test_missing_access_method_rejects_ambiguous_shapes():
    config = BedrockEmbeddingConfig(
        role_arn="arn:aws:iam::123456789012:role/bedrock-access",
        external_id="external-id",
        aws_access_key_id="access-key",
        aws_secret_access_key="secret-key",
        region_name="us-west-2",
    )

    with pytest.raises(ValueError, match="access_method is required"):
        config.get_client_kwargs()


def test_assume_role_requires_role_fields():
    config = BedrockEmbeddingConfig(access_method="assume_role", region_name="us-west-2")

    with pytest.raises(ValueError, match="Bedrock assume_role auth requires"):
        config.run_precheck()


def test_assume_role_ignores_stale_static_credentials():
    config = BedrockEmbeddingConfig(
        access_method="assume_role",
        role_arn="arn:aws:iam::123456789012:role/bedrock-access",
        external_id="external-id",
        aws_access_key_id="stale",
        aws_secret_access_key="stale",
        region_name="us-west-2",
    )

    assert config.get_client_kwargs() == {"region_name": "us-west-2"}


def test_credentials_requires_static_keys():
    config = BedrockEmbeddingConfig(access_method="credentials", region_name="us-west-2")

    with pytest.raises(ValueError, match="Bedrock credentials auth requires"):
        config.get_client_kwargs()
