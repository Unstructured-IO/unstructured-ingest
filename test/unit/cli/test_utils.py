import json
from typing import Optional

import pytest
from pydantic import BaseModel, Secret, SecretStr, ValidationError

from unstructured_ingest.cli.utils.click import extract_config
from unstructured_ingest.cli.utils.model_conversion import options_from_base_model
from unstructured_ingest.interfaces import AccessConfig, ConnectionConfig


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


def test_options_from_base_model_marks_secret_str_as_sensitive():
    class MyModel(BaseModel):
        api_key: SecretStr

    options = options_from_base_model(MyModel)

    assert len(options) == 1
    assert options[0].help == "[sensitive] "


def test_options_from_base_model_marks_secret_nested_model_fields_as_sensitive():
    class MyAccessConfig(AccessConfig):
        token: str

    class MyModel(ConnectionConfig):
        access_config: Secret[MyAccessConfig]

    options = options_from_base_model(MyModel)

    assert len(options) == 1
    assert options[0].name == "token"
    assert options[0].help == "[sensitive] "


def test_options_from_base_model_marks_optional_secret_nested_model_fields_as_sensitive():
    class MyAccessConfig(AccessConfig):
        token: str

    class MyModel(ConnectionConfig):
        access_config: Optional[Secret[MyAccessConfig]]

    options = options_from_base_model(MyModel)

    assert len(options) == 1
    assert options[0].name == "token"
    assert options[0].help == "[sensitive] "
