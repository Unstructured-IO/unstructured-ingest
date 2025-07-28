from abc import ABC
from typing import Any, AsyncGenerator, Generator, Optional, TypeVar

from pydantic import BaseModel

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.interfaces.connector import BaseConnector
from unstructured_ingest.interfaces.process import BaseProcess
from unstructured_ingest.processes.utils.logging.connectors import IndexerConnectorLoggingMixin


class IndexerConfig(BaseModel):
    pass


IndexerConfigT = TypeVar("IndexerConfigT", bound=IndexerConfig)


class Indexer(BaseProcess, BaseConnector, IndexerConnectorLoggingMixin, ABC):
    connector_type: str
    endpoint_to_log: str
    index_config: Optional[IndexerConfigT] = None

    def __post_init__(self):
        IndexerConnectorLoggingMixin.__init__(self)

    def is_async(self) -> bool:
        return False

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        self.log_indexing_start(connector_type=self.connector_type, endpoint=self.endpoint_to_log)
        try:
            response = self._run(**kwargs)
            item_count = 0
            for item in response:
                yield item
                item_count += 1
        except Exception as e:
            self.log_indexing_failed(
                connector_type=self.connector_type, error=e, endpoint=self.endpoint_to_log
            )
            raise self.wrap_error(e=e)
        self.log_indexing_complete(
            connector_type=self.connector_type, endpoint=self.endpoint_to_log, count=item_count
        )

    async def run_async(self, **kwargs: Any) -> AsyncGenerator[FileData, None]:
        self.log_indexing_start(connector_type=self.connector_type, endpoint=self.endpoint_to_log)
        try:
            response = await self._run_async(**kwargs)
            item_count = 0
            async for item in response:
                yield item
                item_count += 1
        except Exception as e:
            self.log_indexing_failed(
                connector_type=self.connector_type, error=e, endpoint=self.endpoint_to_log
            )
            raise self.wrap_error(e=e)
        self.log_indexing_complete(
            connector_type=self.connector_type, endpoint=self.endpoint_to_log, count=item_count
        )

    # TODO: Convert into @abstractmethod once all existing indexers have this implemented
    def _run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        raise NotImplementedError()

    async def _run_async(self, **kwargs: Any) -> AsyncGenerator[FileData, None]:
        return self._run(**kwargs)

    @property
    # TODO: Convert into @abstractmethod once all existing indexers have this property
    def endpoint_to_log(self) -> str:
        return "<ENDPOINT TO SPECIFY>"

    def precheck(self) -> None:
        self.log_connection_validation_start(
            connector_type=self.connector_type, endpoint=self.endpoint_to_log
        )
        try:
            self._precheck()
        except Exception as e:
            self.log_connection_validation_failed(
                connector_type=self.connector_type, error=e, endpoint=self.endpoint_to_log
            )
            raise self.wrap_error(e=e)
        self.log_connection_validation_success(
            connector_type=self.connector_type, endpoint=self.endpoint_to_log
        )

    def _precheck(self) -> None:
        pass

    def wrap_error(self, e: Exception) -> Exception:
        return e
