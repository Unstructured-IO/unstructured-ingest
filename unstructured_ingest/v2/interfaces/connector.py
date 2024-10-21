from abc import ABC
from dataclasses import dataclass
from typing import Any, TypeVar, Union

from pydantic import BaseModel, Secret, model_validator
from pydantic.types import _SecretBase


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
        if self._is_access_config_optional() and access_config is None:
            return self
        if not isinstance(access_config, _SecretBase):
            raise ValueError("access_config must be an instance of SecretBase")
        return self

    def _is_access_config_optional(self) -> bool:
        access_config_type = self.model_fields["access_config"].annotation
        return (
            hasattr(access_config_type, "__origin__")
            and hasattr(access_config_type, "__args__")
            and access_config_type.__origin__ is Union
            and len(access_config_type.__args__) == 2
            and type(None) in access_config_type.__args__
        )


ConnectionConfigT = TypeVar("ConnectionConfigT", bound=ConnectionConfig)


@dataclass
class BaseConnector(ABC):
    connection_config: ConnectionConfigT
