from dataclasses import dataclass

from pydantic import Field, Secret

from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.v2.processes.connectors.qdrant.qdrant import (
    QdrantAccessConfig,
    QdrantConnectionConfig,
    QdrantUploader,
    QdrantUploaderConfig,
    QdrantUploadStager,
    QdrantUploadStagerConfig,
)

CONNECTOR_TYPE = "qdrant-server"


class ServerQdrantAccessConfig(QdrantAccessConfig):
    pass


class ServerQdrantConnectionConfig(QdrantConnectionConfig):
    url: str = Field(default=None, description="url of Qdrant server")
    access_config: Secret[ServerQdrantAccessConfig] = Field(
        default_factory=ServerQdrantAccessConfig, validate_default=True
    )

    def get_client_kwargs(self) -> dict:
        return {
            "url": self.url,
        }


class ServerQdrantUploadStagerConfig(QdrantUploadStagerConfig):
    pass


@dataclass
class ServerQdrantUploadStager(QdrantUploadStager):
    upload_stager_config: ServerQdrantUploadStagerConfig


class ServerQdrantUploaderConfig(QdrantUploaderConfig):
    pass


@dataclass
class ServerQdrantUploader(QdrantUploader):
    connection_config: ServerQdrantConnectionConfig
    upload_config: ServerQdrantUploaderConfig
    connector_type: str = CONNECTOR_TYPE


qdrant_server_destination_entry = DestinationRegistryEntry(
    connection_config=ServerQdrantConnectionConfig,
    uploader=ServerQdrantUploader,
    uploader_config=ServerQdrantUploaderConfig,
    upload_stager=ServerQdrantUploadStager,
    upload_stager_config=ServerQdrantUploadStagerConfig,
)
