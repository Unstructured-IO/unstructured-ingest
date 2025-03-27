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

CONNECTOR_TYPE = "lancedb_azure"


class LanceDBAzureAccessConfig(AccessConfig):
    azure_storage_account_name: str = Field(description="The name of the azure storage account.")
    azure_storage_account_key: str = Field(description="The serialized azure service account key.")


class LanceDBAzureConnectionConfig(LanceDBRemoteConnectionConfig):
    access_config: Secret[LanceDBAzureAccessConfig]

    def get_storage_options(self) -> dict:
        return {**self.access_config.get_secret_value().model_dump(), "timeout": self.timeout}


@dataclass
class LanceDBAzureUploader(LanceDBUploader):
    upload_config: LanceDBUploaderConfig
    connection_config: LanceDBAzureConnectionConfig
    connector_type: str = CONNECTOR_TYPE


lancedb_azure_destination_entry = DestinationRegistryEntry(
    connection_config=LanceDBAzureConnectionConfig,
    uploader=LanceDBAzureUploader,
    uploader_config=LanceDBUploaderConfig,
    upload_stager_config=LanceDBUploadStagerConfig,
    upload_stager=LanceDBUploadStager,
)
