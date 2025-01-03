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

CONNECTOR_TYPE = "databricks_volumes_azure"


class DatabricksAzureVolumesAccessConfig(DatabricksVolumesAccessConfig):
    account_id: Optional[str] = Field(
        default=None,
        description="The Databricks account ID for the Databricks " "accounts endpoint.",
    )
    profile: Optional[str] = None
    azure_workspace_resource_id: Optional[str] = Field(
        default=None,
        description="The Azure Resource Manager ID for the Azure Databricks workspace, "
        "which is exchanged for a Databricks host URL.",
    )
    azure_client_secret: Optional[str] = Field(
        default=None, description="The Azure AD service principal’s client secret."
    )
    azure_client_id: Optional[str] = Field(
        default=None, description="The Azure AD service principal’s application ID."
    )
    azure_tenant_id: Optional[str] = Field(
        default=None, description="The Azure AD service principal’s tenant ID."
    )
    azure_environment: Optional[str] = Field(
        default=None,
        description="The Azure environment type for a " "specific set of API endpoints",
        examples=["Public", "UsGov", "China", "Germany"],
    )


class DatabricksAzureVolumesConnectionConfig(DatabricksVolumesConnectionConfig):
    access_config: Secret[DatabricksAzureVolumesAccessConfig]


class DatabricksAzureVolumesIndexerConfig(DatabricksVolumesIndexerConfig):
    pass


@dataclass
class DatabricksAzureVolumesIndexer(DatabricksVolumesIndexer):
    connection_config: DatabricksAzureVolumesConnectionConfig
    index_config: DatabricksAzureVolumesIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class DatabricksAzureVolumesDownloaderConfig(DatabricksVolumesDownloaderConfig):
    pass


@dataclass
class DatabricksAzureVolumesDownloader(DatabricksVolumesDownloader):
    connection_config: DatabricksAzureVolumesConnectionConfig
    download_config: DatabricksVolumesDownloaderConfig
    connector_type: str = CONNECTOR_TYPE


class DatabricksAzureVolumesUploaderConfig(DatabricksVolumesUploaderConfig):
    pass


@dataclass
class DatabricksAzureVolumesUploader(DatabricksVolumesUploader):
    connection_config: DatabricksAzureVolumesConnectionConfig
    upload_config: DatabricksAzureVolumesUploaderConfig = field(
        default_factory=DatabricksAzureVolumesUploaderConfig
    )
    connector_type: str = CONNECTOR_TYPE


databricks_azure_volumes_destination_entry = DestinationRegistryEntry(
    connection_config=DatabricksAzureVolumesConnectionConfig,
    uploader=DatabricksAzureVolumesUploader,
    uploader_config=DatabricksAzureVolumesUploaderConfig,
)

databricks_azure_volumes_source_entry = SourceRegistryEntry(
    connection_config=DatabricksAzureVolumesConnectionConfig,
    indexer=DatabricksAzureVolumesIndexer,
    indexer_config=DatabricksAzureVolumesIndexerConfig,
    downloader=DatabricksAzureVolumesDownloader,
    downloader_config=DatabricksAzureVolumesDownloaderConfig,
)
