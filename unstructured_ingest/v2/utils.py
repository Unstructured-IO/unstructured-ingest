from typing import Any

from pydantic import BaseModel, Secret


def is_secret(value: Any) -> bool:
    return (
        hasattr(value, "__origin__") and hasattr(value, "__args__") and value.__origin__ is Secret
    )


def serialize_base_model(model: BaseModel) -> dict:
    # To get the full serialized dict regardless of if values are marked as Secret
    model_dict = model.dict()
    for name, field_data in model.__fields__.items():
        field_annotation = field_data.annotation
        if is_secret(field_annotation):
            secret_value = getattr(model, name).get_secret_value()
            model_dict[name] = serialize_base_model(secret_value)

    return model_dict
