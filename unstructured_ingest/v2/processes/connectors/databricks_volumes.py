import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Generator

from pydantic import Field, Secret

from unstructured.documents.elements import DataSourceMetadata
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionNetworkError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
    Indexer,
    IndexerConfig,
    SourceIdentifiers, Downloader, DownloaderConfig
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry, SourceRegistryEntry

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
    cluster_id: Optional[str] = None
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


@dataclass
class DatabricksVolumesIndexerConfig(IndexerConfig):
    remote_url: str
    recursive: bool = False
    catalog: str = Field(init=False)
    path: str = Field(init=False)
    full_name: str = Field(init=False)

    def __post_init__(self):
        full_path = self.remote_url

        if full_path.startswith("/"):
            full_path = full_path[1:]
        parts = full_path.split("/")
        if parts[0] != "Volumes":
            raise ValueError(
                "remote url needs to be of the format /Volumes/catalog_name/volume/path"
            )
        self.catalog = parts[1]
        self.path = "/".join(parts[2:])
        self.full_name = ".".join(parts[1:])


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
class DatabricksVolumesIndexer(Indexer):
    connector_type: str = CONNECTOR_TYPE
    index_config: DatabricksVolumesIndexerConfig
    connection_config: DatabricksVolumesConnectionConfig
    workspace: "WorkspaceClient" = Field(init=False)

    def __post_init__(self):
        self.workspace = self.connection_config.get_client()

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        for file_info in self.workspace.dbfs.list(
                path=self.index_config.remote_url, recursive=self.index_config.recursive
        ):
            if file_info.is_dir:
                continue
            rel_path = file_info.path.replace(self.index_config.remote_url, "")
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
                    additional_metadata={
                        "catalog": self.index_config.catalog,
                    },
                ),
                metadata=DataSourceMetadata(
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
    workspace: "WorkspaceClient" = Field(init=False)

    def __post_init__(self):
        self.workspace = self.connection_config.get_client()

    def get_download_path(self, file_data: FileData) -> Path:
        return self.download_config.download_dir / Path(file_data.source_identifiers.relative_path)

    @staticmethod
    def is_float(value: str):
        try:
            float(value)
            return True
        except ValueError:
            return False

    def run(self, file_data: FileData, **kwargs: Any) -> Path:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Writing {file_data.identifier} to {download_path}")
        try:
            with self.workspace.dbfs.download(path=file_data.identifier) as c:
                read_content = c._read_handle.read()
            with open(download_path, "wb") as f:
                f.write(read_content)
        except Exception as e:
            logger.error(f"failed to download file {file_data.identifier}: {e}", exc_info=True)
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")
        if (
                file_data.metadata.date_modified
                and self.is_float(file_data.metadata.date_modified)
                and file_data.metadata.date_created
                and self.is_float(file_data.metadata.date_created)
        ):
            date_modified = float(file_data.metadata.date_modified)
            date_created = float(file_data.metadata.date_created)
            os.utime(download_path, times=(date_created, date_modified))
        return download_path


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
            **self.connection_config.access_config.get_secret_value().model_dump(),
        )

    def precheck(self) -> None:
        try:
            assert self.get_client().current_user.me().active
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        output_path = os.path.join(self.upload_config.path, path.name)
        with open(path, "rb") as elements_file:
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

databricks_volumes_source_entry = SourceRegistryEntry(
    connection_config=DatabricksVolumesConnectionConfig,
    indexer=DatabricksVolumesIndexer,
    indexer_config=DatabricksVolumesIndexerConfig,
    downloader=DatabricksVolumesDownloader,
    downloader_config=DatabricksVolumesDownloaderConfig,
)

