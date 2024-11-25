import random

import faker
import pytest

from unstructured_ingest.v2.processes.chunker import Chunker, ChunkerConfig

fake = faker.Faker()


def generate_chunker_config_params() -> dict:
    params = {}
    random_val = random.random()
    if random_val < 0.5:
        params["chunking_strategy"] = fake.word() if random.random() < 0.5 else None
        params["chunk_combine_text_under_n_chars"] = (
            fake.random_int() if random.random() < 0.5 else None
        )
        params["chunk_include_orig_elements"] = fake.boolean() if random.random() < 0.5 else None
        params["chunk_max_characters"] = fake.random_int()
        params["chunk_multipage_sections"] = fake.boolean()
        params["chunk_new_after_n_chars"] = fake.random_int() if random.random() < 0.5 else None
        params["chunk_overlap"] = fake.random_int() if random.random() < 0.5 else None
        params["chunk_overlap_all"] = fake.boolean() if random.random() < 0.5 else None
    if random_val < 0.5:
        params["chunk_by_api"] = True
        params["chunking_endpoint"] = fake.url()
        params["chunk_api_key"] = fake.password()
    else:
        params["chunk_by_api"] = False

    return params


@pytest.mark.parametrize(
    "partition_config_params", [generate_chunker_config_params() for i in range(10)]
)
def test_chunker_config(partition_config_params: dict):
    chunker_config = ChunkerConfig.model_validate(partition_config_params)
    assert chunker_config


@pytest.mark.parametrize(
    "partition_config_params", [generate_chunker_config_params() for i in range(10)]
)
def test_chunker(partition_config_params: dict):
    chunker_config = ChunkerConfig.model_validate(partition_config_params)
    chunker = Chunker(config=chunker_config)
    assert chunker
