from dataclasses import dataclass

from pydantic import Field, Secret

from unstructured_ingest.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.processes.connectors.qdrant.qdrant import (
    QdrantAccessConfig,
    QdrantConnectionConfig,
    QdrantUploader,
    QdrantUploaderConfig,
    QdrantUploadStager,
    QdrantUploadStagerConfig,
)

CONNECTOR_TYPE = "qdrant-local"


class LocalQdrantAccessConfig(QdrantAccessConfig):
    pass


class LocalQdrantConnectionConfig(QdrantConnectionConfig):
    path: str = Field(default=None, description="Persistence path for QdrantLocal.")
    access_config: Secret[LocalQdrantAccessConfig] = Field(
        default_factory=LocalQdrantAccessConfig, validate_default=True
    )

    def get_client_kwargs(self) -> dict:
        return {"path": self.path}


class LocalQdrantUploadStagerConfig(QdrantUploadStagerConfig):
    pass


@dataclass
class LocalQdrantUploadStager(QdrantUploadStager):
    upload_stager_config: LocalQdrantUploadStagerConfig


class LocalQdrantUploaderConfig(QdrantUploaderConfig):
    pass


@dataclass
class LocalQdrantUploader(QdrantUploader):
    connection_config: LocalQdrantConnectionConfig
    upload_config: LocalQdrantUploaderConfig
    connector_type: str = CONNECTOR_TYPE


qdrant_local_destination_entry = DestinationRegistryEntry(
    connection_config=LocalQdrantConnectionConfig,
    uploader=LocalQdrantUploader,
    uploader_config=LocalQdrantUploaderConfig,
    upload_stager=LocalQdrantUploadStager,
    upload_stager_config=LocalQdrantUploadStagerConfig,
)
