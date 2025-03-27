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

CONNECTOR_TYPE = "lancedb_gcs"


class LanceDBGCSAccessConfig(AccessConfig):
    google_service_account_key: str = Field(
        description="The serialized google service account key."
    )


class LanceDBGCSConnectionConfig(LanceDBRemoteConnectionConfig):
    access_config: Secret[LanceDBGCSAccessConfig]

    def get_storage_options(self) -> dict:
        return {**self.access_config.get_secret_value().model_dump(), "timeout": self.timeout}


@dataclass
class LanceDBGSPUploader(LanceDBUploader):
    upload_config: LanceDBUploaderConfig
    connection_config: LanceDBGCSConnectionConfig
    connector_type: str = CONNECTOR_TYPE


lancedb_gcp_destination_entry = DestinationRegistryEntry(
    connection_config=LanceDBGCSConnectionConfig,
    uploader=LanceDBGSPUploader,
    uploader_config=LanceDBUploaderConfig,
    upload_stager_config=LanceDBUploadStagerConfig,
    upload_stager=LanceDBUploadStager,
)
