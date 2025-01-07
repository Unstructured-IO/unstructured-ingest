from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import BaseModel, Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import FileData
from unstructured_ingest.v2.logger import logger
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

if TYPE_CHECKING:
    from databricks.sql import Connection as DeltaTableConnection
    from databricks.sql.client import Cursor as DeltaTableCursor

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


class TableMigrationConfig(BaseModel):
    database: Optional[str] = Field(
        description="Database name", default="default", alias="migration_database"
    )
    table: Optional[str] = Field(description="Table name", default=None, alias="migration_table")
    http_path: Optional[str] = Field(
        description="http path connection config value", default=None, alias="migration_http_path"
    )

    def is_set(self) -> bool:
        return self.database and self.table is not None and self.http_path is not None

    @contextmanager
    @requires_dependencies(["databricks"], extras="databricks-delta-tables")
    def get_connection(
        self, connection_configs: DatabricksNativeVolumesConnectionConfig
    ) -> Generator["DeltaTableConnection", None, None]:
        from databricks.sdk.core import Config, oauth_service_principal
        from databricks.sql import connect

        connect_kwargs = {
            "server_hostname": connection_configs.host,
            "http_path": self.http_path,
        }

        host = f"https://{connection_configs.host}"
        access_configs = connection_configs.access_config.get_secret_value()
        credential_provider = oauth_service_principal(
            Config(
                host=host,
                client_id=access_configs.client_id,
                client_secret=access_configs.client_secret,
            )
        )

        connect_kwargs["credentials_provider"] = credential_provider
        with connect(**connect_kwargs) as connection:
            yield connection

    @contextmanager
    def get_cursor(
        self, connection_configs: DatabricksNativeVolumesConnectionConfig
    ) -> Generator["DeltaTableCursor", None, None]:
        with self.get_connection(connection_configs=connection_configs) as connection:
            cursor = connection.cursor()
            yield cursor


class DatabricksNativeVolumesUploaderConfig(DatabricksVolumesUploaderConfig):
    table_migration_config: Optional[TableMigrationConfig] = Field(
        description="Configuration for migrating data to a table in Databricks", default=None
    )


@dataclass
class DatabricksNativeVolumesUploader(DatabricksVolumesUploader):
    connection_config: DatabricksNativeVolumesConnectionConfig
    upload_config: DatabricksNativeVolumesUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        super().run(path=path, file_data=file_data, **kwargs)
        output_path = self.get_output_path(file_data=file_data)
        if self.upload_config.table_migration_config.is_set():
            table_migration_config = self.upload_config.table_migration_config
            logger.info(
                f"migrating uploaded file {output_path} into table {table_migration_config.table}"
            )
            with table_migration_config.get_cursor(
                connection_configs=self.connection_config
            ) as cursor:
                cursor.execute(f"USE CATALOG '{self.upload_config.catalog}'")
                cursor.execute(
                    f"SELECT * FROM json.{output_path} INSERT INTO {table_migration_config.table}"
                )


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
