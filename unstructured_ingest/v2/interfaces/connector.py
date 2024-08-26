from abc import ABC
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel, Secret


class AccessConfig(BaseModel):
    """Meant to designate holding any sensitive information associated with other configs
    and also for access specific configs."""


AccessConfigT = TypeVar("AccessConfigT", bound=AccessConfig)


class ConnectionConfig(BaseModel):
    access_config: Secret[AccessConfigT]

    def get_access_config(self) -> dict[str, Any]:
        if not self.access_config:
            return {}
        return self.access_config.get_secret_value().dict()


ConnectionConfigT = TypeVar("ConnectionConfigT", bound=ConnectionConfig)


@dataclass
class BaseConnector(ABC):
    connection_config: ConnectionConfigT
