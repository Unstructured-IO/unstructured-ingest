import random
from typing import Any

import faker
import pytest

from test.unit.v2.utils.data_generator import generate_random_dictionary
from unstructured_ingest.embed.huggingface import (
    HuggingFaceEmbeddingConfig,
    HuggingFaceEmbeddingEncoder,
)

fake = faker.Faker()


def generate_embedder_config_params() -> dict:
    params = {}
    if random.random() < 0.5:
        params["embed_model_name"] = fake.word() if random.random() < 0.5 else None
        params["embedder_model_kwargs"] = (
            generate_random_dictionary(key_type=str, value_type=Any)
            if random.random() < 0.5
            else None
        )
        params["encode_kwargs"] = (
            generate_random_dictionary(key_type=str, value_type=Any)
            if random.random() < 0.5
            else None
        )
        params["cache_folder"] = fake.file_path() if random.random() < 0.5 else None
    return params


@pytest.mark.parametrize(
    "embedder_config_params", [generate_embedder_config_params() for i in range(10)]
)
def test_embedder_config(embedder_config_params: dict):
    embedder_config = HuggingFaceEmbeddingConfig.model_validate(embedder_config_params)
    assert embedder_config


@pytest.mark.parametrize(
    "embedder_config_params", [generate_embedder_config_params() for i in range(10)]
)
def test_embedder(embedder_config_params: dict):
    embedder_config = HuggingFaceEmbeddingConfig.model_validate(embedder_config_params)
    embedder = HuggingFaceEmbeddingEncoder(config=embedder_config)
    assert embedder
