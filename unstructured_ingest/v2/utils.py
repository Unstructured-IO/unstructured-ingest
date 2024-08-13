from typing import Any

from pydantic import BaseModel, field_serializer
from pydantic.types import _SecretBase


def is_secret(value: Any) -> bool:
    return (
        hasattr(value, "__origin__")
        and hasattr(value, "__args__")
        and issubclass(value.__origin__, _SecretBase)
    ) or issubclass(value, _SecretBase)


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
    model_fields = model.model_fields
    model_secret_fields = {k: v for k, v in model_fields.items() if is_secret(v.annotation)}

    class CustomBaseModel(type(model)):
        @field_serializer(*model_secret_fields.keys(), when_used="json")
        def dump_secret(self, v):
            return v.get_secret_value()

    custom_model = CustomBaseModel.model_validate(obj=model.dict())

    print(custom_model.json(**json_kwargs))
