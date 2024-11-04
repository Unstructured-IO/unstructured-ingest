import contextlib
import datetime
from collections import Counter
from enum import EnumMeta
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Callable,
    Literal,
    Optional,
    Type,
    TypedDict,
    Union,
    get_args,
    get_origin,
)
from uuid import UUID

import click
from annotated_types import Ge, Gt, Le, Lt, SupportsGe, SupportsGt, SupportsLe, SupportsLt
from click import Option
from pydantic import BaseModel, Secret, SecretStr
from pydantic.fields import FieldInfo
from pydantic.types import _SecretBase
from pydantic_core import PydanticUndefined

from unstructured_ingest.v2.cli.utils.click import (
    DelimitedString,
    Dict,
    PydanticDate,
    PydanticDateTime,
)

NoneType = type(None)


class _RangeDict(TypedDict, total=False):
    """Represent arguments to `click.IntRange` or `click.FloatRange`."""

    max: Union[SupportsLt, SupportsLe]
    min: Union[SupportsGt, SupportsGe]
    max_open: bool
    min_open: bool


def get_range_from_metadata(metadata: list[Any]) -> _RangeDict:
    range_args: _RangeDict = {}
    for constraint in metadata:
        if isinstance(constraint, Le):
            range_args["max"] = constraint.le
            range_args["max_open"] = False
        if isinstance(constraint, Lt):
            range_args["max"] = constraint.lt
            range_args["max_open"] = True
        if isinstance(constraint, Ge):
            range_args["min"] = constraint.ge
            range_args["min_open"] = False
        if isinstance(constraint, Gt):
            range_args["min"] = constraint.gt
            range_args["min_open"] = True
    return range_args


def is_boolean_flag(field_info: FieldInfo) -> bool:
    annotation = field_info.annotation
    raw_annotation = get_raw_type(annotation)
    return raw_annotation is bool


def get_raw_type(val: Any) -> Any:
    field_args = get_args(val)
    field_origin = get_origin(val)
    if field_origin is Union and len(field_args) == 2 and NoneType in field_args:
        field_type = next(field_arg for field_arg in field_args if field_arg is not None)
        return field_type
    if field_origin is Secret and len(field_args) == 1:
        field_type = next(field_arg for field_arg in field_args if field_arg is not None)
        return field_type
    if val is SecretStr:
        return str
    return val


def get_default_value_from_field(field: FieldInfo) -> Optional[Union[Any, Callable[[], Any]]]:
    if field.default is not PydanticUndefined:
        return field.default
    elif field.default_factory is not None:
        return field.default_factory
    return None


def get_option_name(field_name: str, field_info: FieldInfo) -> str:
    field_name = field_info.alias or field_name
    if field_name.startswith("--"):
        field_name = field_name[2:]
    field_name = field_name.lower().replace("_", "-")
    if is_boolean_flag(field_info):
        return f"--{field_name}/--no-{field_name}"
    return f"--{field_name}"


def get_numerical_type(field: FieldInfo) -> click.ParamType:
    range_args = get_range_from_metadata(field.metadata)
    if field.annotation is int:
        if range_args:
            return click.IntRange(**range_args)  # type: ignore[arg-type]
        return click.INT
    # Non-integer numerical types default to float
    if range_args:
        return click.FloatRange(**range_args)  # type: ignore[arg-type]
    return click.FLOAT


def get_type_from_annotation(field_type: Any) -> click.ParamType:
    field_origin = get_origin(field_type)
    field_args = get_args(field_type)
    if field_origin is Union and len(field_args) == 2 and NoneType in field_args:
        field_type = next(field_arg for field_arg in field_args if field_arg is not None)
        return get_type_from_annotation(field_type=field_type)
    if field_origin is Annotated:
        field_origin = field_args[0]
        field_metadata = field_args[1]
        if isinstance(field_metadata, click.ParamType):
            return field_metadata
    if field_origin is Secret and len(field_args) == 1:
        field_type = next(field_arg for field_arg in field_args if field_arg is not None)
        return get_type_from_annotation(field_type=field_type)
    if field_origin is list and len(field_args) == 1 and field_args[0] is str:
        return DelimitedString()
    if field_type is SecretStr:
        return click.STRING
    if dict in [field_type, field_origin]:
        return Dict()
    if field_type is str:
        return click.STRING
    if field_type is bool:
        return click.BOOL
    if field_type is UUID:
        return click.UUID
    if field_type is Path:
        return click.Path(path_type=Path)
    if field_type is datetime.datetime:
        return PydanticDateTime()
    if field_type is datetime.date:
        return PydanticDate()
    if field_origin is Literal:
        return click.Choice(field_args)
    if isinstance(field_type, EnumMeta):
        values = [i.value for i in field_type]
        return click.Choice(values)
    raise TypeError(f"Unexpected field type: {field_type}")


def _get_type_from_field(field: FieldInfo) -> click.ParamType:
    raw_field_type = get_raw_type(field.annotation)

    if raw_field_type in (int, float):
        return get_numerical_type(field)
    return get_type_from_annotation(field_type=field.annotation)


def get_option_from_field(option_name: str, field_info: FieldInfo) -> Option:
    param_decls = [option_name]
    help_text = field_info.description or ""
    if examples := field_info.examples:
        help_text += f" [Examples: {', '.join(examples)}]"
    option_kwargs = {
        "type": _get_type_from_field(field_info),
        "default": get_default_value_from_field(field_info),
        "required": field_info.is_required(),
        "help": str(help_text),
        "is_flag": is_boolean_flag(field_info),
        "show_default": field_info.default is not PydanticUndefined,
    }
    return click.Option(param_decls=param_decls, **option_kwargs)


def is_subclass(x: Any, y: Any) -> bool:
    with contextlib.suppress(TypeError):
        return issubclass(x, y)

    return False


def post_check(options: list[Option]):
    option_names = [option.name for option in options]
    duplicate_names = [name for name, count in Counter(option_names).items() if count > 1]
    if duplicate_names:
        raise ValueError(
            "the following field name were reused, all must be unique: {}".format(
                ", ".join(duplicate_names)
            )
        )


def is_secret(value: Any) -> bool:
    # Case Secret[int]
    if hasattr(value, "__origin__") and hasattr(value, "__args__"):
        origin = value.__origin__
        return is_subclass(origin, _SecretBase)
    # Case SecretStr
    return is_subclass(value, _SecretBase)


def options_from_base_model(model: Union[BaseModel, Type[BaseModel]]) -> list[Option]:
    options = []
    model_fields = model.model_fields
    for field_name, field_info in model_fields.items():
        if field_info.init is False:
            continue
        option_name = get_option_name(field_name=field_name, field_info=field_info)
        raw_annotation = get_raw_type(field_info.annotation)
        if is_subclass(raw_annotation, BaseModel):
            options.extend(options_from_base_model(model=raw_annotation))
        else:
            if is_secret(field_info.annotation):
                field_info.description = f"[sensitive] {field_info.description}"
            options.append(get_option_from_field(option_name=option_name, field_info=field_info))

    post_check(options=options)
    return options
