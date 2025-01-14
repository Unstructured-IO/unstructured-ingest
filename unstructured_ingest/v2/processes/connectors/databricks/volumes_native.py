from dataclasses import dataclass
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

CONNECTOR_TYPE = "databricks_volumes"


class DatabricksNativeVolumesAccessConfig(DatabricksVolumesAccessConfig):
    client_id: Optional[str] = Field(default=None, description="Client ID of the OAuth app.")
    client_secret: Optional[str] = Field(
        default=None, description="Client Secret of the OAuth app."
    )
    profile: Optional[str] = None
    azure_workspace_resource_id: Optional[str] = Field(
        default=None,
        description="The Azure Resource Manager ID for the Azure Databricks workspace, "
        "which is exchanged for a Databricks host URL.",
    )


class DatabricksNativeVolumesConnectionConfig(DatabricksVolumesConnectionConfig):
    access_config: Secret[DatabricksNativeVolumesAccessConfig]


class DatabricksNativeVolumesIndexerConfig(DatabricksVolumesIndexerConfig):
    pass


@dataclass
class DatabricksNativeVolumesIndexer(DatabricksVolumesIndexer):
    connection_config: DatabricksNativeVolumesConnectionConfig
    index_config: DatabricksNativeVolumesIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class DatabricksNativeVolumesDownloaderConfig(DatabricksVolumesDownloaderConfig):
    pass


@dataclass
class DatabricksNativeVolumesDownloader(DatabricksVolumesDownloader):
    connection_config: DatabricksNativeVolumesConnectionConfig
    download_config: DatabricksVolumesDownloaderConfig
    connector_type: str = CONNECTOR_TYPE


class DatabricksNativeVolumesUploaderConfig(DatabricksVolumesUploaderConfig):
    pass


@dataclass
class DatabricksNativeVolumesUploader(DatabricksVolumesUploader):
    connection_config: DatabricksNativeVolumesConnectionConfig
    upload_config: DatabricksNativeVolumesUploaderConfig
    connector_type: str = CONNECTOR_TYPE


databricks_native_volumes_destination_entry = DestinationRegistryEntry(
    connection_config=DatabricksNativeVolumesConnectionConfig,
    uploader=DatabricksNativeVolumesUploader,
    uploader_config=DatabricksNativeVolumesUploaderConfig,
)

databricks_native_volumes_source_entry = SourceRegistryEntry(
    connection_config=DatabricksNativeVolumesConnectionConfig,
    indexer=DatabricksNativeVolumesIndexer,
    indexer_config=DatabricksNativeVolumesIndexerConfig,
    downloader=DatabricksNativeVolumesDownloader,
    downloader_config=DatabricksNativeVolumesDownloaderConfig,
)
