import json
from datetime import datetime
from inspect import isclass
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_DNS, uuid5

from pydantic import BaseModel
from pydantic.types import _SecretBase

from unstructured_ingest.v2.interfaces import FileData


def is_secret(value: Any) -> bool:
    # Case Secret[int]
    if hasattr(value, "__origin__") and hasattr(value, "__args__"):
        origin = value.__origin__
        return isclass(origin) and issubclass(origin, _SecretBase)
    # Case SecretStr
    return isclass(value) and issubclass(value, _SecretBase)


def serialize_base_model(model: BaseModel) -> dict:
    # To get the full serialized dict regardless of if values are marked as Secret
    model_dict = model.model_dump()
    return serialize_base_dict(model_dict=model_dict)


def serialize_base_dict(model_dict: dict) -> dict:
    model_dict = model_dict.copy()
    for k, v in model_dict.items():
        if isinstance(v, _SecretBase):
            secret_value = v.get_secret_value()
            if isinstance(secret_value, BaseModel):
                model_dict[k] = serialize_base_model(model=secret_value)
            else:
                model_dict[k] = secret_value
        if isinstance(v, dict):
            model_dict[k] = serialize_base_dict(model_dict=v)

    return model_dict


def serialize_base_model_json(model: BaseModel, **json_kwargs) -> str:
    model_dict = serialize_base_model(model=model)

    def json_serial(obj):
        if isinstance(obj, Path):
            return obj.as_posix()
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))

    # Support json dumps kwargs such as sort_keys
    return json.dumps(model_dict, default=json_serial, **json_kwargs)


def get_enhanced_element_id(element_dict: dict, file_data: FileData) -> str:
    element_id = element_dict.get("element_id")
    new_data = f"{element_id}{file_data.identifier}"
    return str(uuid5(NAMESPACE_DNS, new_data))
