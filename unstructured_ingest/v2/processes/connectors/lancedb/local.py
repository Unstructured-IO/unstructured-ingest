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

CONNECTOR_TYPE = "lancedb_local"


class LanceDBLocalAccessConfig(AccessConfig):
    pass


class LanceDBLocalConnectionConfig(LanceDBConnectionConfig):
    access_config: Secret[LanceDBLocalAccessConfig] = Field(
        default_factory=LanceDBLocalAccessConfig, validate_default=True
    )

    def get_storage_options(self) -> None:
        return None


@dataclass
class LanceDBLocalUploader(LanceDBUploader):
    upload_config: LanceDBUploaderConfig
    connection_config: LanceDBLocalConnectionConfig
    connector_type: str = CONNECTOR_TYPE


lancedb_local_destination_entry = DestinationRegistryEntry(
    connection_config=LanceDBLocalConnectionConfig,
    uploader=LanceDBLocalUploader,
    uploader_config=LanceDBUploaderConfig,
    upload_stager_config=LanceDBUploadStagerConfig,
    upload_stager=LanceDBUploadStager,
)
