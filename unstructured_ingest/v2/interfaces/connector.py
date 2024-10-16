from abc import ABC
from dataclasses import dataclass
from typing import Any, TypeVar

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
        if not isinstance(access_config, _SecretBase):
            raise ValueError("access_config must be an instance of SecretBase")
        return self


ConnectionConfigT = TypeVar("ConnectionConfigT", bound=ConnectionConfig)


@dataclass
class BaseConnector(ABC):
    connection_config: ConnectionConfigT
