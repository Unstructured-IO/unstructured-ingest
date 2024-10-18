import random
from typing import Any

import faker
import pytest

from test.unit.v2.utils.data_generator import generate_random_dictionary
from unstructured_ingest.v2.processes.partitioner import Partitioner, PartitionerConfig

fake = faker.Faker()


def generate_partitioner_config_params() -> dict:
    params = {
        "strategy": random.choice(["fast", "hi_res", "auto"]),
        "ocr_languages": fake.words() if random.random() < 0.5 else None,
        "encoding": fake.word() if random.random() < 0.5 else None,
        "additional_partition_args": (
            generate_random_dictionary(key_type=str, value_type=Any)
            if random.random() < 0.5
            else None
        ),
        "skip_infer_table_types": fake.words() if random.random() < 0.5 else None,
        "flatten_metadata": fake.boolean(),
        "hi_res_model_name": fake.word() if random.random() < 0.5 else None,
    }
    random_val = random.random()
    # Randomly set the fields_include to a random list[str]
    if random_val < 0.5:
        params["fields_include"] = fake.words()

    # Randomly set the metadata_exclude or metadata_include to a valid
    # list[str] or don't set it at all
    if random.random() < (1 / 3):
        params["metadata_exclude"] = fake.words()
    elif random_val < (2 / 3):
        params["metadata_include"] = fake.words()

    # Randomly set the values associated with calling the api, or not at all
    if random.random() < 0.5:
        params["partition_by_api"]: True
        params["partition_endpoint"] = fake.url()
        params["api_key"] = fake.password()
    else:
        params["partition_by_api"]: False
    return params


@pytest.mark.parametrize(
    "partition_config_params", [generate_partitioner_config_params() for i in range(10)]
)
def test_partition_config(partition_config_params: dict):
    partition_config = PartitionerConfig.model_validate(partition_config_params)
    assert partition_config


@pytest.mark.parametrize(
    "partition_config_params", [generate_partitioner_config_params() for i in range(10)]
)
def test_partitioner(partition_config_params: dict):
    partition_config = PartitionerConfig.model_validate(partition_config_params)
    partitioner = Partitioner(config=partition_config)
    assert partitioner
