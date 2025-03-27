from dataclasses import dataclass

from pydantic import Field, Secret

from unstructured_ingest.interfaces.connector import AccessConfig
from unstructured_ingest.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.processes.connectors.lancedb.lancedb import (
    LanceDBRemoteConnectionConfig,
    LanceDBUploader,
    LanceDBUploaderConfig,
    LanceDBUploadStager,
    LanceDBUploadStagerConfig,
)

CONNECTOR_TYPE = "lancedb_cloud"


class LanceDBCloudAccessConfig(AccessConfig):
    api_key: str = Field(description="Api key associated with LanceDb cloud")


class LanceDBCloudConnectionConfig(LanceDBRemoteConnectionConfig):
    access_config: Secret[LanceDBCloudAccessConfig]

    def get_storage_options(self) -> dict:
        return {**self.access_config.get_secret_value().model_dump(), "timeout": self.timeout}


@dataclass
class LanceDBCloudUploader(LanceDBUploader):
    upload_config: LanceDBUploaderConfig
    connection_config: LanceDBCloudConnectionConfig
    connector_type: str = CONNECTOR_TYPE


lancedb_cloud_destination_entry = DestinationRegistryEntry(
    connection_config=LanceDBCloudConnectionConfig,
    uploader=LanceDBCloudUploader,
    uploader_config=LanceDBUploaderConfig,
    upload_stager_config=LanceDBUploadStagerConfig,
    upload_stager=LanceDBUploadStager,
)
