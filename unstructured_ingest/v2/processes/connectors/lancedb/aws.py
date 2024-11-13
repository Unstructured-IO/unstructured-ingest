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

CONNECTOR_TYPE = "lancedb_s3"


class LanceDBS3AccessConfig(AccessConfig):
    aws_access_key_id: str = Field(description="The AWS access key ID to use.")
    aws_secret_access_key: str = Field(description="The AWS secret access key to use.")


class LanceDBS3ConnectionConfig(LanceDBConnectionConfig):
    access_config: Secret[LanceDBS3AccessConfig]


@dataclass
class LanceDBS3Uploader(LanceDBUploader):
    upload_config: LanceDBUploaderConfig
    connection_config: LanceDBS3ConnectionConfig
    connector_type: str = CONNECTOR_TYPE


lancedb_aws_destination_entry = DestinationRegistryEntry(
    connection_config=LanceDBS3ConnectionConfig,
    uploader=LanceDBS3Uploader,
    uploader_config=LanceDBUploaderConfig,
    upload_stager_config=LanceDBUploadStagerConfig,
    upload_stager=LanceDBUploadStager,
)
