from abc import ABC
from dataclasses import dataclass
from typing import Any


@dataclass
class BaseProcess(ABC):
    def is_async(self) -> bool:
        return False

    def init(self, **kwargs: Any) -> None:
        pass

    def precheck(self) -> None:
        pass

    def run(self, **kwargs: Any) -> Any:
        raise NotImplementedError()

    async def run_async(self, **kwargs: Any) -> Any:
        return self.run(**kwargs)
