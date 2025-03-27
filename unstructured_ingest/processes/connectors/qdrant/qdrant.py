import asyncio
import json
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, AsyncGenerator, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.error import DestinationConnectionError, WriteError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.data_prep import (
    batch_generator,
    flatten_dict,
    get_enhanced_element_id,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from qdrant_client import AsyncQdrantClient, QdrantClient


class QdrantAccessConfig(AccessConfig, ABC):
    pass


class QdrantConnectionConfig(ConnectionConfig, ABC):
    access_config: Secret[QdrantAccessConfig] = Field(
        default_factory=QdrantAccessConfig, validate_default=True, description="Access Config"
    )

    @abstractmethod
    def get_client_kwargs(self) -> dict:
        pass

    @requires_dependencies(["qdrant_client"], extras="qdrant")
    @asynccontextmanager
    async def get_async_client(self) -> AsyncGenerator["AsyncQdrantClient", None]:
        from qdrant_client import AsyncQdrantClient

        client_kwargs = self.get_client_kwargs()
        client = AsyncQdrantClient(**client_kwargs)
        try:
            yield client
        finally:
            await client.close()

    @requires_dependencies(["qdrant_client"], extras="qdrant")
    @contextmanager
    def get_client(self) -> Generator["QdrantClient", None, None]:
        from qdrant_client import QdrantClient

        client_kwargs = self.get_client_kwargs()
        client = QdrantClient(**client_kwargs)
        try:
            yield client
        finally:
            client.close()


class QdrantUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class QdrantUploadStager(UploadStager, ABC):
    upload_stager_config: QdrantUploadStagerConfig = field(
        default_factory=lambda: QdrantUploadStagerConfig()
    )

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        """Prepares dictionary in the format that Chroma requires"""
        data = element_dict.copy()
        return {
            "id": get_enhanced_element_id(element_dict=data, file_data=file_data),
            "vector": data.pop("embeddings", {}),
            "payload": {
                "text": data.pop("text", None),
                "element_serialized": json.dumps(data),
                **flatten_dict(
                    data,
                    separator="-",
                    flatten_lists=True,
                ),
            },
        }


class QdrantUploaderConfig(UploaderConfig):
    collection_name: str = Field(description="Name of the collection.")
    batch_size: int = Field(default=50, description="Number of records per batch.")
    num_processes: Optional[int] = Field(
        default=1,
        description="Optional limit on number of threads to use for upload.",
        deprecated=True,
    )


@dataclass
class QdrantUploader(Uploader, ABC):
    upload_config: QdrantUploaderConfig
    connection_config: QdrantConnectionConfig

    @DestinationConnectionError.wrap
    def precheck(self) -> None:
        with self.connection_config.get_client() as client:
            collections_response = client.get_collections()
            collection_names = [c.name for c in collections_response.collections]
            if self.upload_config.collection_name not in collection_names:
                raise DestinationConnectionError(
                    "collection '{}' not found: {}".format(
                        self.upload_config.collection_name, ", ".join(collection_names)
                    )
                )

    def is_async(self):
        return True

    async def run_data_async(
        self,
        data: list[dict],
        file_data: FileData,
        **kwargs: Any,
    ) -> None:
        batches = list(batch_generator(data, batch_size=self.upload_config.batch_size))
        logger.debug(
            "Elements split into %i batches of size %i.",
            len(batches),
            self.upload_config.batch_size,
        )
        await asyncio.gather(*[self._upsert_batch(batch) for batch in batches])

    async def _upsert_batch(self, batch: list[dict]) -> None:
        from qdrant_client import models

        points: list[models.PointStruct] = [models.PointStruct(**item) for item in batch]
        try:
            logger.debug(
                "Upserting %i points to the '%s' collection.",
                len(points),
                self.upload_config.collection_name,
            )
            async with self.connection_config.get_async_client() as async_client:
                await async_client.upsert(
                    self.upload_config.collection_name, points=points, wait=True
                )
        except Exception as api_error:
            logger.error(
                "Failed to upsert points to the collection due to the following error %s", api_error
            )

            raise WriteError(f"Qdrant error: {api_error}") from api_error

        logger.debug("Successfully upsert points to the collection.")
