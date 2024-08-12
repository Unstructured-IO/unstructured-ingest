import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    UploadContent,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

CONNECTOR_TYPE = "databricks_volumes"


class DatabricksVolumesAccessConfig(AccessConfig):
    account_id: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    token: Optional[str] = None
    profile: Optional[str] = None
    azure_workspace_resource_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_tenant_id: Optional[str] = None
    azure_environment: Optional[str] = None
    auth_type: Optional[str] = None
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
    host: Optional[str] = None


class DatabricksVolumesUploaderConfig(UploaderConfig):
    volume: str
    catalog: str
    volume_path: Optional[str] = None
    overwrite: bool = False
    databricks_schema: str = Field(default="default", alias="schema")

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

    def run(self, contents: list[UploadContent], **kwargs: Any) -> None:
        for content in contents:
            with open(content.path, "rb") as elements_file:
                output_path = os.path.join(self.upload_config.path, content.path.name)
                self.get_client().files.upload(
                    file_path=output_path,
                    contents=elements_file,
                    overwrite=self.upload_config.overwrite,
                )


databricks_volumes_destination_entry = DestinationRegistryEntry(
    connection_config=DatabricksVolumesConnectionConfig,
    uploader=DatabricksVolumesUploader,
    uploader_config=DatabricksVolumesUploaderConfig,
)
