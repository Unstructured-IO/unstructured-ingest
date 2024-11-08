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

CONNECTOR_TYPE = "qdrant-cloud"


class CloudQdrantAccessConfig(QdrantAccessConfig):
    api_key: str = Field(description="Qdrant API key")


class CloudQdrantConnectionConfig(QdrantConnectionConfig):
    url: str = Field(default=None, description="url of Qdrant Cloud")
    access_config: Secret[CloudQdrantAccessConfig]

    def get_client_kwargs(self) -> dict:
        return {
            "api_key": self.access_config.get_secret_value().api_key,
            "url": self.url,
        }


class CloudQdrantUploadStagerConfig(QdrantUploadStagerConfig):
    pass


@dataclass
class CloudQdrantUploadStager(QdrantUploadStager):
    upload_stager_config: CloudQdrantUploadStagerConfig


class CloudQdrantUploaderConfig(QdrantUploaderConfig):
    pass


@dataclass
class CloudQdrantUploader(QdrantUploader):
    connection_config: CloudQdrantConnectionConfig
    upload_config: CloudQdrantUploaderConfig
    connector_type: str = CONNECTOR_TYPE


qdrant_cloud_destination_entry = DestinationRegistryEntry(
    connection_config=CloudQdrantConnectionConfig,
    uploader=CloudQdrantUploader,
    uploader_config=CloudQdrantUploaderConfig,
    upload_stager=CloudQdrantUploadStager,
    upload_stager_config=CloudQdrantUploadStagerConfig,
)
