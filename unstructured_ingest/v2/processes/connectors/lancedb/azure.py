from dataclasses import dataclass

from pydantic import Field, Secret

from unstructured_ingest.v2.interfaces.connector import AccessConfig
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.v2.processes.connectors.lancedb.lancedb import (
    LanceDBConnectionConfig,
    LanceDBUploader,
    LanceDBUploaderConfig,
    LanceDBUploadStager,
    LanceDBUploadStagerConfig,
)

CONNECTOR_TYPE = "lancedb_azure"


class LanceDBAzureAccessConfig(AccessConfig):
    azure_storage_account_name: str = Field(description="The name of the azure storage account.")
    azure_storage_account_key: str = Field(description="The serialized azure service account key.")


class LanceDBAzureConnectionConfig(LanceDBConnectionConfig):
    access_config: Secret[LanceDBAzureAccessConfig]


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
