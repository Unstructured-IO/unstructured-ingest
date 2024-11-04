import asyncio
import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

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
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

if TYPE_CHECKING:
    from qdrant_client import AsyncQdrantClient


CONNECTOR_TYPE = "qdrant"


class QdrantAccessConfig(AccessConfig):
    api_key: Optional[str] = Field(default=None, description="API Key")


class QdrantConnectionConfig(ConnectionConfig):
    collection_name: str = Field(description="Collection Name")
    location: Optional[str] = Field(default=None, description="Location")
    url: Optional[str] = Field(default=None, description="URL")
    port: Optional[int] = Field(default=6333, description="Port")
    grpc_port: Optional[int] = Field(default=6334, description="GRPC Port")
    prefer_grpc: Optional[bool] = Field(default=False, description="Prefer GRCP")
    https: Optional[bool] = Field(default=None, description="HTTPS")
    prefix: Optional[str] = Field(default=None, description="Prefix")
    timeout: Optional[float] = Field(default=None, description="TimeOut time")
    host: Optional[str] = Field(default=None, description="Host")
    path: Optional[str] = Field(default=None, description="Path")
    force_disable_check_same_thread: Optional[bool] = Field(
        default=False, description="Force disable check for same thread"
    )
    access_config: Secret[QdrantAccessConfig] = Field(default=None, description="Access Config")

    @requires_dependencies(["qdrant_client"], extras="qdrant")
    def get_async_client(self) -> "AsyncQdrantClient":
        from qdrant_client.async_qdrant_client import AsyncQdrantClient

        client = AsyncQdrantClient(
            location=self.location,
            url=self.url,
            port=self.port,
            grpc_port=self.grpc_port,
            prefer_grpc=self.prefer_grpc,
            https=self.https,
            api_key=(self.access_config.get_secret_value().api_key if self.access_config else None),
            prefix=self.prefix,
            timeout=self.timeout,
            host=self.host,
            path=self.path,
            force_disable_check_same_thread=self.force_disable_check_same_thread,
        )

        return client


class QdrantUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class QdrantUploadStager(UploadStager):
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
    batch_size: int = Field(default=50, description="Number of records per batch")
    num_processes: Optional[int] = Field(
        default=1,
        description="Optional limit on number of threads to use for upload",
        deprecated=True,
    )


@dataclass
class QdrantUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: QdrantUploaderConfig
    connection_config: QdrantConnectionConfig

    @DestinationConnectionError.wrap
    def precheck(self) -> None:
        async def check_connection():
            async_client = self.connection_config.get_async_client()
            await async_client.get_collections()

        asyncio.run(check_connection())

    def is_async(self):
        return True

    async def run_async(
        self,
        path: Path,
        file_data: FileData,
        **kwargs: Any,
    ) -> Path:
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

        client = self.connection_config.get_async_client()
        points: list[models.PointStruct] = [models.PointStruct(**item) for item in batch]
        try:
            logger.debug(
                "Upserting %i points to the '%s' collection.",
                len(points),
                self.connection_config.collection_name,
            )
            response = await client.upsert(
                self.connection_config.collection_name, points=points, wait=True
            )
        except Exception as api_error:
            logger.debug("Upsert response: %s", response)
            logger.error(
                "Failed to upsert points to the collection due to the following error %s", api_error
            )

            raise WriteError(f"Qdrant error: {api_error}") from api_error

        logger.debug("Successfully upsert points to the collection.")


qdrant_destination_entry = DestinationRegistryEntry(
    connection_config=QdrantConnectionConfig,
    uploader=QdrantUploader,
    uploader_config=QdrantUploaderConfig,
    upload_stager=QdrantUploadStager,
    upload_stager_config=QdrantUploadStagerConfig,
)
