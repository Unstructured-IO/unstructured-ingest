from abc import ABC, abstractmethod
from typing import Any, Generator, Optional, TypeVar

from pydantic import BaseModel

from unstructured_ingest.v2.interfaces.connector import BaseConnector
from unstructured_ingest.v2.interfaces.file_data import FileData
from unstructured_ingest.v2.interfaces.process import BaseProcess


class IndexerConfig(BaseModel):
    pass


IndexerConfigT = TypeVar("IndexerConfigT", bound=IndexerConfig)


class Indexer(BaseProcess, BaseConnector, ABC):
    connector_type: str
    index_config: Optional[IndexerConfigT] = None

    def is_async(self) -> bool:
        return False

    @abstractmethod
    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        pass
