import json
from typing import Optional

import pytest
from pydantic import BaseModel, Secret, ValidationError

from unstructured_ingest.cli.utils.click import Dict, extract_config
from unstructured_ingest.cli.utils.model_conversion import get_type_from_annotation
from unstructured_ingest.interfaces import AccessConfig, ConnectionConfig


def test_list_of_models_field_parses_json_array_on_cli():
    # a list of pydantic models maps to Dict() (json-loads the inline JSON array), and
    # the parsed value must validate back into the model list
    class Item(BaseModel):
        a: str

    param_type = get_type_from_annotation(list[Item])
    assert isinstance(param_type, Dict)
    parsed = param_type.convert('[{"a": "x"}, {"a": "y"}]')
    assert [Item(**i) for i in parsed] == [Item(a="x"), Item(a="y")]


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
