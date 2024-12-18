from dataclasses import dataclass, field
from typing import Optional

from pydantic import Field, Secret

from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.databricks.volumes import (
    DatabricksVolumesAccessConfig,
    DatabricksVolumesConnectionConfig,
    DatabricksVolumesDownloader,
    DatabricksVolumesDownloaderConfig,
    DatabricksVolumesIndexer,
    DatabricksVolumesIndexerConfig,
    DatabricksVolumesUploader,
    DatabricksVolumesUploaderConfig,
)

CONNECTOR_TYPE = "databricks_volumes_aws"


class DatabricksAWSVolumesAccessConfig(DatabricksVolumesAccessConfig):
    account_id: Optional[str] = Field(
        default=None,
        description="The Databricks account ID for the Databricks " "accounts endpoint",
    )
    profile: Optional[str] = None
    token: Optional[str] = Field(
        default=None,
        description="The Databricks personal access token (PAT)",
    )


class DatabricksAWSVolumesConnectionConfig(DatabricksVolumesConnectionConfig):
    access_config: Secret[DatabricksAWSVolumesAccessConfig]


class DatabricksAWSVolumesIndexerConfig(DatabricksVolumesIndexerConfig):
    pass


@dataclass
class DatabricksAWSVolumesIndexer(DatabricksVolumesIndexer):
    connection_config: DatabricksAWSVolumesConnectionConfig
    index_config: DatabricksAWSVolumesIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class DatabricksAWSVolumesDownloaderConfig(DatabricksVolumesDownloaderConfig):
    pass


@dataclass
class DatabricksAWSVolumesDownloader(DatabricksVolumesDownloader):
    connection_config: DatabricksAWSVolumesConnectionConfig
    download_config: DatabricksVolumesDownloaderConfig
    connector_type: str = CONNECTOR_TYPE


class DatabricksAWSVolumesUploaderConfig(DatabricksVolumesUploaderConfig):
    pass


@dataclass
class DatabricksAWSVolumesUploader(DatabricksVolumesUploader):
    connection_config: DatabricksAWSVolumesConnectionConfig
    upload_config: DatabricksAWSVolumesUploaderConfig = field(
        default_factory=DatabricksAWSVolumesUploaderConfig
    )
    connector_type: str = CONNECTOR_TYPE


databricks_aws_volumes_destination_entry = DestinationRegistryEntry(
    connection_config=DatabricksAWSVolumesConnectionConfig,
    uploader=DatabricksAWSVolumesUploader,
    uploader_config=DatabricksAWSVolumesUploaderConfig,
)

databricks_aws_volumes_source_entry = SourceRegistryEntry(
    connection_config=DatabricksAWSVolumesConnectionConfig,
    indexer=DatabricksAWSVolumesIndexer,
    indexer_config=DatabricksAWSVolumesIndexerConfig,
    downloader=DatabricksAWSVolumesDownloader,
    downloader_config=DatabricksAWSVolumesDownloaderConfig,
)
