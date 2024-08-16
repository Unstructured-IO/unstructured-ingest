import json
from datetime import datetime
from inspect import isclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from pydantic.types import _SecretBase


def is_secret(value: Any) -> bool:
    # Case Secret[int]
    if hasattr(value, "__origin__") and hasattr(value, "__args__"):
        origin = value.__origin__
        return isclass(origin) and issubclass(origin, _SecretBase)
    # Case SecretStr
    return isclass(value) and issubclass(value, _SecretBase)


def serialize_base_model(model: BaseModel) -> dict:
    # To get the full serialized dict regardless of if values are marked as Secret
    model_dict = model.dict()
    for k, v in model_dict.items():
        if isinstance(v, _SecretBase):
            secret_value = v.get_secret_value()
            if isinstance(secret_value, BaseModel):
                model_dict[k] = serialize_base_model(model=secret_value)
            else:
                model_dict[k] = secret_value

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
