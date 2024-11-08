import asyncio
import json
import uuid
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError, WriteError
from unstructured_ingest.utils.data_prep import batch_generator, flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.v2.logger import logger

if TYPE_CHECKING:
    from qdrant_client import AsyncQdrantClient


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
    async def get_client(self) -> AsyncGenerator["AsyncQdrantClient", None]:
        from qdrant_client.async_qdrant_client import AsyncQdrantClient

        client_kwargs = self.get_client_kwargs()
        client = AsyncQdrantClient(**client_kwargs)
        try:
            yield client
        finally:
            await client.close()


class QdrantUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class QdrantUploadStager(UploadStager, ABC):
    upload_stager_config: QdrantUploadStagerConfig = field(
        default_factory=lambda: QdrantUploadStagerConfig()
    )

    @staticmethod
    def conform_dict(data: dict) -> dict:
        """Prepares dictionary in the format that Chroma requires"""
        return {
            "id": str(uuid.uuid4()),
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

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)

        conformed_elements = [self.conform_dict(data=element) for element in elements_contents]
        output_path = Path(output_dir) / Path(f"{output_filename}.json")

        with open(output_path, "w") as output_file:
            json.dump(conformed_elements, output_file)
        return output_path


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
        async def check_connection():
            async with self.connection_config.get_client() as async_client:
                await async_client.get_collections()

        asyncio.run(check_connection())

    def is_async(self):
        return True

    async def run_async(
        self,
        path: Path,
        file_data: FileData,
        **kwargs: Any,
    ) -> None:
        with path.open("r") as file:
            elements: list[dict] = json.load(file)

        logger.debug("Loaded %i elements from %s", len(elements), path)

        batches = list(batch_generator(elements, batch_size=self.upload_config.batch_size))
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
            async with self.connection_config.get_client() as async_client:
                await async_client.upsert(
                    self.upload_config.collection_name, points=points, wait=True
                )
        except Exception as api_error:
            logger.error(
                "Failed to upsert points to the collection due to the following error %s", api_error
            )

            raise WriteError(f"Qdrant error: {api_error}") from api_error

        logger.debug("Successfully upsert points to the collection.")
