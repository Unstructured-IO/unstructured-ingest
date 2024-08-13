from typing import Any

from pydantic import BaseModel, Secret
from pydantic.types import _SecretBase


def is_secret(value: Any) -> bool:
    return (
        hasattr(value, "__origin__") and hasattr(value, "__args__") and value.__origin__ is Secret
    )


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
