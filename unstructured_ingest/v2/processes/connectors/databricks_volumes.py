import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

CONNECTOR_TYPE = "databricks_volumes"


class DatabricksVolumesAccessConfig(AccessConfig):
    account_id: Optional[str] = Field(
        default=None,
        description="The Databricks account ID for the Databricks "
        "accounts endpoint. Only has effect when Host is "
        "either https://accounts.cloud.databricks.com/ (AWS), "
        "https://accounts.azuredatabricks.net/ (Azure), "
        "or https://accounts.gcp.databricks.com/ (GCP).",
    )
    username: Optional[str] = Field(
        default=None,
        description="The Databricks username part of basic authentication. "
        "Only possible when Host is *.cloud.databricks.com (AWS).",
    )
    password: Optional[str] = Field(
        default=None,
        description="The Databricks password part of basic authentication. "
        "Only possible when Host is *.cloud.databricks.com (AWS).",
    )
    client_id: Optional[str] = Field(default=None)
    client_secret: Optional[str] = Field(default=None)
    token: Optional[str] = Field(
        default=None,
        description="The Databricks personal access token (PAT) (AWS, Azure, and GCP) or "
        "Azure Active Directory (Azure AD) token (Azure).",
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
    auth_type: Optional[str] = Field(
        default=None,
        description="When multiple auth attributes are available in the "
        "environment, use the auth type specified by this "
        "argument. This argument also holds the currently "
        "selected auth.",
    )
    cluster_id: Optional[str] = None
    google_credentials: Optional[str] = None
    google_service_account: Optional[str] = None


SecretDatabricksVolumesAccessConfig = Secret[DatabricksVolumesAccessConfig]


class DatabricksVolumesConnectionConfig(ConnectionConfig):
    access_config: SecretDatabricksVolumesAccessConfig = Field(
        default_factory=lambda: SecretDatabricksVolumesAccessConfig(
            secret_value=DatabricksVolumesAccessConfig()
        )
    )
    host: Optional[str] = Field(
        default=None,
        description="The Databricks host URL for either the "
        "Databricks workspace endpoint or the "
        "Databricks accounts endpoint.",
    )


class DatabricksVolumesUploaderConfig(UploaderConfig):
    volume: str = Field(description="Name of volume in the Unity Catalog")
    catalog: str = Field(description="Name of the catalog in the Databricks Unity Catalog service")
    volume_path: Optional[str] = Field(
        default=None, description="Optional path within the volume to write to"
    )
    overwrite: bool = Field(
        default=False, description="If true, an existing file will be overwritten."
    )
    databricks_schema: str = Field(
        default="default",
        alias="schema",
        description="Schema associated with the volume to write to in the Unity Catalog service",
    )

    @property
    def path(self) -> str:
        path = f"/Volumes/{self.catalog}/{self.databricks_schema}/{self.volume}"
        if self.volume_path:
            path = f"{path}/{self.volume_path}"
        return path


@dataclass
class DatabricksVolumesUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: DatabricksVolumesUploaderConfig
    connection_config: DatabricksVolumesConnectionConfig

    @requires_dependencies(dependencies=["databricks.sdk"], extras="databricks-volumes")
    def get_client(self) -> "WorkspaceClient":
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient(
            host=self.connection_config.host,
            **self.connection_config.access_config.get_secret_value().dict(),
        )

    def precheck(self) -> None:
        try:
            assert self.get_client().current_user.me().active
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        output_path = os.path.join(self.upload_config.path, path.name)
        self.get_client().files.upload(
            file_path=output_path,
            contents=path,
            overwrite=self.upload_config.overwrite,
        )


databricks_volumes_destination_entry = DestinationRegistryEntry(
    connection_config=DatabricksVolumesConnectionConfig,
    uploader=DatabricksVolumesUploader,
    uploader_config=DatabricksVolumesUploaderConfig,
)
