import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)

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
    client_id: Optional[str] = Field(default=None, description="Client ID of the OAuth app.")
    client_secret: Optional[str] = Field(
        default=None, description="Client Secret of the OAuth app."
    )
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
    google_credentials: Optional[str] = None
    google_service_account: Optional[str] = None


class DatabricksVolumesConnectionConfig(ConnectionConfig):
    access_config: Secret[DatabricksVolumesAccessConfig] = Field(
        default=DatabricksVolumesAccessConfig(), validate_default=True
    )
    host: Optional[str] = Field(
        default=None,
        description="The Databricks host URL for either the "
        "Databricks workspace endpoint or the "
        "Databricks accounts endpoint.",
    )
    volume: str = Field(description="Name of volume in the Unity Catalog")
    catalog: str = Field(description="Name of the catalog in the Databricks Unity Catalog service")
    volume_path: Optional[str] = Field(
        default=None, description="Optional path within the volume to write to"
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

    @requires_dependencies(dependencies=["databricks.sdk"], extras="databricks-volumes")
    def get_client(self) -> "WorkspaceClient":
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient(
            host=self.host,
            **self.access_config.get_secret_value().model_dump(),
        )


@dataclass
class DatabricksVolumesIndexerConfig(IndexerConfig):
    recursive: bool = False


@dataclass
class DatabricksVolumesIndexer(Indexer):
    index_config: DatabricksVolumesIndexerConfig
    connection_config: DatabricksVolumesConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            self.connection_config.get_client()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        for file_info in self.connection_config.get_client().dbfs.list(
            path=self.connection_config.path, recursive=self.index_config.recursive
        ):
            if file_info.is_dir:
                continue
            rel_path = file_info.path.replace(self.connection_config.path, "")
            if rel_path.startswith("/"):
                rel_path = rel_path[1:]
            filename = Path(file_info.path).name
            yield FileData(
                identifier=file_info.path,
                connector_type=CONNECTOR_TYPE,
                source_identifiers=SourceIdentifiers(
                    filename=filename,
                    rel_path=rel_path,
                    fullpath=file_info.path,
                ),
                additional_metadata={
                    "catalog": self.connection_config.catalog,
                },
                metadata=FileDataSourceMetadata(
                    url=file_info.path, date_modified=str(file_info.modification_time)
                ),
            )


@dataclass
class DatabricksVolumesDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class DatabricksVolumesDownloader(Downloader):
    download_config: DatabricksVolumesDownloaderConfig
    connection_config: DatabricksVolumesConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            self.connection_config.get_client()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def get_download_path(self, file_data: FileData) -> Path:
        return self.download_config.download_dir / Path(file_data.source_identifiers.relative_path)

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Writing {file_data.identifier} to {download_path}")
        try:
            with self.connection_config.get_client().dbfs.download(path=file_data.identifier) as c:
                read_content = c._read_handle.read()
            with open(download_path, "wb") as f:
                f.write(read_content)
        except Exception as e:
            logger.error(f"failed to download file {file_data.identifier}: {e}", exc_info=True)
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")

        return self.generate_download_response(file_data=file_data, download_path=download_path)


class DatabricksVolumesUploaderConfig(UploaderConfig):
    overwrite: bool = Field(
        default=False, description="If true, an existing file will be overwritten."
    )


@dataclass
class DatabricksVolumesUploader(Uploader):
    upload_config: DatabricksVolumesUploaderConfig
    connection_config: DatabricksVolumesConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            assert self.connection_config.get_client().current_user.me().active
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        output_path = os.path.join(self.connection_config.path, path.name)
        with open(path, "rb") as elements_file:
            self.connection_config.get_client().files.upload(
                file_path=output_path,
                contents=elements_file,
                overwrite=self.upload_config.overwrite,
            )


databricks_volumes_destination_entry = DestinationRegistryEntry(
    connection_config=DatabricksVolumesConnectionConfig,
    uploader=DatabricksVolumesUploader,
    uploader_config=DatabricksVolumesUploaderConfig,
)

databricks_volumes_source_entry = SourceRegistryEntry(
    connection_config=DatabricksVolumesConnectionConfig,
    indexer=DatabricksVolumesIndexer,
    indexer_config=DatabricksVolumesIndexerConfig,
    downloader=DatabricksVolumesDownloader,
    downloader_config=DatabricksVolumesDownloaderConfig,
)
