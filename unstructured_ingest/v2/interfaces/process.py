from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class BaseProcess(ABC):
    def is_async(self) -> bool:
        return False

    def init(self, *kwargs: Any) -> None:
        pass

    def precheck(self) -> None:
        pass

    @abstractmethod
    def run(self, **kwargs: Any) -> Any:
        pass

    async def run_async(self, **kwargs: Any) -> Any:
        return self.run(**kwargs)
