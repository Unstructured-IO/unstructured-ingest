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

CONNECTOR_TYPE = "databricks_volumes_gcp"


class DatabricksGoogleVolumesAccessConfig(DatabricksVolumesAccessConfig):
    account_id: Optional[str] = Field(
        default=None,
        description="The Databricks account ID for the Databricks " "accounts endpoint.",
    )
    profile: Optional[str] = None
    google_credentials: Optional[str] = None
    google_service_account: Optional[str] = None


class DatabricksGoogleVolumesConnectionConfig(DatabricksVolumesConnectionConfig):
    access_config: Secret[DatabricksGoogleVolumesAccessConfig]


class DatabricksGoogleVolumesIndexerConfig(DatabricksVolumesIndexerConfig):
    pass


@dataclass
class DatabricksGoogleVolumesIndexer(DatabricksVolumesIndexer):
    connection_config: DatabricksGoogleVolumesConnectionConfig
    index_config: DatabricksGoogleVolumesIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class DatabricksGoogleVolumesDownloaderConfig(DatabricksVolumesDownloaderConfig):
    pass


@dataclass
class DatabricksGoogleVolumesDownloader(DatabricksVolumesDownloader):
    connection_config: DatabricksGoogleVolumesConnectionConfig
    download_config: DatabricksVolumesDownloaderConfig
    connector_type: str = CONNECTOR_TYPE


class DatabricksGoogleVolumesUploaderConfig(DatabricksVolumesUploaderConfig):
    pass


@dataclass
class DatabricksGoogleVolumesUploader(DatabricksVolumesUploader):
    connection_config: DatabricksGoogleVolumesConnectionConfig
    upload_config: DatabricksGoogleVolumesUploaderConfig = field(
        default_factory=DatabricksGoogleVolumesUploaderConfig
    )
    connector_type: str = CONNECTOR_TYPE


databricks_gcp_volumes_destination_entry = DestinationRegistryEntry(
    connection_config=DatabricksGoogleVolumesConnectionConfig,
    uploader=DatabricksGoogleVolumesUploader,
    uploader_config=DatabricksGoogleVolumesUploaderConfig,
)

databricks_gcp_volumes_source_entry = SourceRegistryEntry(
    connection_config=DatabricksGoogleVolumesConnectionConfig,
    indexer=DatabricksGoogleVolumesIndexer,
    indexer_config=DatabricksGoogleVolumesIndexerConfig,
    downloader=DatabricksGoogleVolumesDownloader,
    downloader_config=DatabricksGoogleVolumesDownloaderConfig,
)
