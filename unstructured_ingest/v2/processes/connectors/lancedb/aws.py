from dataclasses import dataclass

from pydantic import Field, Secret

from unstructured_ingest.v2.interfaces.connector import AccessConfig
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.v2.processes.connectors.lancedb.lancedb import (
    LanceDBRemoteConnectionConfig,
    LanceDBUploader,
    LanceDBUploaderConfig,
    LanceDBUploadStager,
    LanceDBUploadStagerConfig,
)

CONNECTOR_TYPE = "lancedb_aws"


class LanceDBAwsAccessConfig(AccessConfig):
    aws_access_key_id: str = Field(description="The AWS access key ID to use.")
    aws_secret_access_key: str = Field(description="The AWS secret access key to use.")


class LanceDBAwsConnectionConfig(LanceDBRemoteConnectionConfig):
    access_config: Secret[LanceDBAwsAccessConfig]

    def get_storage_options(self) -> dict:
        return {**self.access_config.get_secret_value().model_dump(), "timeout": self.timeout}


@dataclass
class LanceDBAwsUploader(LanceDBUploader):
    upload_config: LanceDBUploaderConfig
    connection_config: LanceDBAwsConnectionConfig
    connector_type: str = CONNECTOR_TYPE


lancedb_aws_destination_entry = DestinationRegistryEntry(
    connection_config=LanceDBAwsConnectionConfig,
    uploader=LanceDBAwsUploader,
    uploader_config=LanceDBUploaderConfig,
    upload_stager_config=LanceDBUploadStagerConfig,
    upload_stager=LanceDBUploadStager,
)
