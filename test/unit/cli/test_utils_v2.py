import json
from typing import Optional

import pytest
from pydantic import Secret, ValidationError

from unstructured_ingest.v2.cli.utils.click import extract_config
from unstructured_ingest.v2.interfaces import AccessConfig, ConnectionConfig


def test_extract_config_optional_access_config():
    class MyAccessConfig(AccessConfig):
        host: str

    class MyModel(ConnectionConfig):
        v: int
        access_config: Optional[Secret[MyAccessConfig]]

    data = {
        "v": 4,
        "host": "localhost",
    }
    extracted_config = extract_config(data, MyModel)
    assert json.loads(extracted_config.model_dump_json()) == {"v": 4, "access_config": "**********"}

    data = {"v": 4}
    extracted_config = extract_config(data, MyModel)
    assert json.loads(extracted_config.model_dump_json()) == {"v": 4, "access_config": None}


def test_extract_config():
    class MyAccessConfig(AccessConfig):
        host: str

    class MyModel(ConnectionConfig):
        v: int
        access_config: Secret[MyAccessConfig]

    data = {
        "v": 4,
        "host": "localhost",
    }
    extracted_config = extract_config(data, MyModel)
    assert json.loads(extracted_config.model_dump_json()) == {"v": 4, "access_config": "**********"}


def test_extract_config_missing_data():
    class MyAccessConfig(AccessConfig):
        host: str

    class MyModel(ConnectionConfig):
        v: int
        access_config: Secret[MyAccessConfig]

    data = {
        "v": 4,
    }
    with pytest.raises(ValidationError):
        extract_config(data, MyModel)
