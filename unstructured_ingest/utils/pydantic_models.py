import json
from datetime import datetime
from pathlib import Path
from typing import Any, get_args, get_origin

from pydantic import BaseModel, Secret, SecretStr


def is_secret_annotation(value: Any) -> bool:
    if get_origin(value) is Secret or value is SecretStr:
        return True
    return any(is_secret_annotation(arg) for arg in get_args(value))


def is_secret_value(value: Any) -> bool:
    return callable(getattr(value, "get_secret_value", None))


def is_secret(value: Any) -> bool:
    return is_secret_annotation(value)


def serialize_base_model(model: BaseModel) -> dict:
    # To get the full serialized dict regardless of if values are marked as Secret
    model_dict = model.model_dump()
    return serialize_base_dict(model_dict=model_dict)


def serialize_base_dict(model_dict: dict) -> dict:
    model_dict = model_dict.copy()
    for k, v in model_dict.items():
        if is_secret_value(v):
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
