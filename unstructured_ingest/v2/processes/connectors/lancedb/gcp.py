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

CONNECTOR_TYPE = "lancedb_gcs"


class LanceDBGCSAccessConfig(AccessConfig):
    google_service_account_key: str = Field(
        description="The serialized google service account key."
    )


class LanceDBGCSConnectionConfig(LanceDBConnectionConfig):
    access_config: Secret[LanceDBGCSAccessConfig]


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
