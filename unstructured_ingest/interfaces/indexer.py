from abc import ABC
from typing import Any, AsyncGenerator, Generator, Optional, TypeVar

from pydantic import BaseModel

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.interfaces.connector import BaseConnector
from unstructured_ingest.interfaces.process import BaseProcess


class IndexerConfig(BaseModel):
    pass


IndexerConfigT = TypeVar("IndexerConfigT", bound=IndexerConfig)


class Indexer(BaseProcess, BaseConnector, ABC):
    connector_type: str
    index_config: Optional[IndexerConfigT] = None

    def is_async(self) -> bool:
        return False

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        raise NotImplementedError()

    async def run_async(self, **kwargs: Any) -> AsyncGenerator[FileData, None]:
        raise NotImplementedError()
