import json
from typing import Any

from pydantic import BaseModel, Field, Secret, SecretStr
from pydantic.types import _SecretBase

from unstructured_ingest.v2.utils import serialize_base_model, serialize_base_model_json


class MockChildBaseModel(BaseModel):
    child_secret_str: SecretStr
    child_secret_float: Secret[float]
    child_not_secret_dict: dict[str, Any] = Field(default_factory=dict)


class MockBaseModel(BaseModel):
    secret_str: SecretStr
    not_secret_bool: bool
    secret_child_base: Secret[MockChildBaseModel]
    not_secret_list: list[int] = Field(default_factory=list)


model = MockBaseModel(
    secret_str="secret string",
    not_secret_bool=False,
    secret_child_base=MockChildBaseModel(
        child_secret_str="child secret string",
        child_secret_float=3.14,
        child_not_secret_dict={"key": "value"},
    ),
    not_secret_list=[1, 2, 3],
)


def test_serialize_base_model():

    serialized_dict = model.model_dump()
    assert isinstance(serialized_dict["secret_str"], _SecretBase)
    assert isinstance(serialized_dict["secret_child_base"], _SecretBase)

    serialized_dict_w_secrets = serialize_base_model(model=model)
    assert not isinstance(serialized_dict_w_secrets["secret_str"], _SecretBase)
    assert not isinstance(serialized_dict_w_secrets["secret_child_base"], _SecretBase)

    expected_dict = {
        "secret_str": "secret string",
        "not_secret_bool": False,
        "secret_child_base": {
            "child_secret_str": "child secret string",
            "child_secret_float": 3.14,
            "child_not_secret_dict": {"key": "value"},
        },
        "not_secret_list": [1, 2, 3],
    }

    assert serialized_dict_w_secrets == expected_dict


def test_serialize_base_model_json():
    serialized_json = model.model_dump_json()
    serialized_dict = json.loads(serialized_json)
    expected_dict = {
        "secret_str": "**********",
        "not_secret_bool": False,
        "secret_child_base": "**********",
        "not_secret_list": [1, 2, 3],
    }
    assert expected_dict == serialized_dict

    serialized_json_w_secrets = serialize_base_model_json(model=model)
    serialized_dict_w_secrets = json.loads(serialized_json_w_secrets)
    expected_dict_w_secrets = {
        "secret_str": "secret string",
        "not_secret_bool": False,
        "secret_child_base": {
            "child_secret_str": "child secret string",
            "child_secret_float": 3.14,
            "child_not_secret_dict": {"key": "value"},
        },
        "not_secret_list": [1, 2, 3],
    }
    assert expected_dict_w_secrets == serialized_dict_w_secrets
