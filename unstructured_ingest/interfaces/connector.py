from abc import ABC
from dataclasses import dataclass
from typing import Any, TypeVar, get_args, get_origin, Union

from pydantic import BaseModel, Secret, model_validator
from pydantic.fields import FieldInfo
from pydantic.types import _SecretBase

from unstructured_ingest.processes.utils.logging.connector import ConnectorLoggingMixin


def _is_optional_field(field: FieldInfo) -> bool:
    """Check whether a pydantic FieldInfo represents an Optional type."""
    origin = get_origin(field.annotation)
    if origin is Union:
        args = get_args(field.annotation)
        return type(None) in args
    return False


class AccessConfig(BaseModel):
    """Meant to designate holding any sensitive information associated with other configs
    and also for access specific configs."""


AccessConfigT = TypeVar("AccessConfigT", bound=AccessConfig)


class ConnectionConfig(BaseModel):
    access_config: Secret[AccessConfigT]

    def get_access_config(self) -> dict[str, Any]:
        if not self.access_config:
            return {}
        return self.access_config.get_secret_value().model_dump()

    @model_validator(mode="after")
    def check_access_config(self):
        access_config = self.access_config
        field = self.__class__.model_fields["access_config"]
        if _is_optional_field(field) and access_config is None:
            return self
        if not isinstance(access_config, _SecretBase):
            raise ValueError("access_config must be an instance of SecretBase")
        return self


ConnectionConfigT = TypeVar("ConnectionConfigT", bound=ConnectionConfig)


@dataclass
class BaseConnector(ABC, ConnectorLoggingMixin):
    connection_config: ConnectionConfigT

    def __post_init__(self):
        """Initialize the logging mixin after dataclass initialization."""
        ConnectorLoggingMixin.__init__(self)
