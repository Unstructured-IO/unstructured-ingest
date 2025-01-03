from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.sql.sql import (
    SQLAccessConfig,
    SQLConnectionConfig,
    SQLUploader,
    SQLUploaderConfig,
    SQLUploadStager,
    SQLUploadStagerConfig,
)

if TYPE_CHECKING:
    from databricks.sdk.core import oauth_service_principal
    from databricks.sql import Connection as DeltaTableConnection
    from databricks.sql.client import Cursor as DeltaTableCursor

CONNECTOR_TYPE = "databricks_delta_tables"


class DatabrickDeltaTablesAccessConfig(SQLAccessConfig):
    token: Optional[str] = Field(default=None, description="Databricks Personal Access Token")
    client_id: Optional[str] = Field(default=None, description="Client ID of the OAuth app.")
    client_secret: Optional[str] = Field(
        default=None, description="Client Secret of the OAuth app."
    )


class DatabrickDeltaTablesConnectionConfig(SQLConnectionConfig):
    access_config: Secret[DatabrickDeltaTablesAccessConfig]
    server_hostname: str = Field(description="server hostname connection config value")
    http_path: str = Field(description="http path connection config value")

    @requires_dependencies(["databricks"], extras="databricks-delta-tables")
    def get_credentials_provider(self) -> "oauth_service_principal":
        from databricks.sdk.core import Config, oauth_service_principal

        host = f"https://{self.server_hostname}"
        access_configs = self.access_config.get_secret_value()
        if (client_id := access_configs.client_id) and (
            client_secret := access_configs.client_secret
        ):
            return oauth_service_principal(
                Config(
                    host=host,
                    client_id=client_id,
                    client_secret=client_secret,
                )
            )
        return False

    def model_post_init(self, __context: Any) -> None:
        access_config = self.access_config.get_secret_value()
        if access_config.token and access_config.client_secret and access_config.client_id:
            raise ValueError(
                "One one for of auth can be provided, either token or client id and secret"
            )
        if not access_config.token and not (
            access_config.client_secret and access_config.client_id
        ):
            raise ValueError(
                "One form of auth must be provided, either token or client id and secret"
            )

    @contextmanager
    @requires_dependencies(["databricks"], extras="databricks-delta-tables")
    def get_connection(self) -> Generator["DeltaTableConnection", None, None]:
        from databricks.sql import connect

        connect_kwargs = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
        }

        if credential_provider := self.get_credentials_provider():
            connect_kwargs["credentials_provider"] = credential_provider
        else:
            connect_kwargs["access_token"] = self.access_config.get_secret_value().token
        with connect(**connect_kwargs) as connection:
            yield connection

    @contextmanager
    def get_cursor(self) -> Generator["DeltaTableCursor", None, None]:
        with self.get_connection() as connection:
            cursor = connection.cursor()
            yield cursor


class DatabrickDeltaTablesUploadStagerConfig(SQLUploadStagerConfig):
    pass


class DatabrickDeltaTablesUploadStager(SQLUploadStager):
    upload_stager_config: DatabrickDeltaTablesUploadStagerConfig


class DatabrickDeltaTablesUploaderConfig(SQLUploaderConfig):
    catalog: str = Field(description="Name of the catalog in the Databricks Unity Catalog service")
    database: str = Field(description="Database name", default="default")
    table: str = Field(description="Table name")


@dataclass
class DatabrickDeltaTablesUploader(SQLUploader):
    upload_config: DatabrickDeltaTablesUploaderConfig = field(
        default_factory=DatabrickDeltaTablesUploaderConfig
    )
    connection_config: DatabrickDeltaTablesConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @contextmanager
    def get_cursor(self) -> Generator[Any, None, None]:
        with self.connection_config.get_cursor() as cursor:
            cursor.execute(f"USE CATALOG '{self.upload_config.catalog}'")
            yield cursor

    def precheck(self) -> None:
        with self.connection_config.get_cursor() as cursor:
            cursor.execute("SHOW CATALOGS")
            catalogs = [r[0] for r in cursor.fetchall()]
            if self.upload_config.catalog not in catalogs:
                raise ValueError(
                    "Catalog {} not found in {}".format(
                        self.upload_config.catalog, ", ".join(catalogs)
                    )
                )
            cursor.execute(f"USE CATALOG '{self.upload_config.catalog}'")
            cursor.execute("SHOW DATABASES")
            databases = [r[0] for r in cursor.fetchall()]
            if self.upload_config.database not in databases:
                raise ValueError(
                    "Database {} not found in {}".format(
                        self.upload_config.database, ", ".join(databases)
                    )
                )
            cursor.execute("SHOW TABLES")
            table_names = [r[1] for r in cursor.fetchall()]
            if self.upload_config.table_name not in table_names:
                raise ValueError(
                    "Table {} not found in {}".format(
                        self.upload_config.table_name, ", ".join(table_names)
                    )
                )

    def prepare_data(
        self, columns: list[str], data: tuple[tuple[Any, ...], ...]
    ) -> list[tuple[Any, ...]]:
        prepared_data = super().prepare_data(columns=columns, data=data)
        output = []
        for row in prepared_data:
            parsed = []
            for val in row:
                if isinstance(val, list):
                    parsed.append("Array({})".format(",".join([str(v) for v in val])))
                else:
                    parsed.append(val)
            output.append(tuple(parsed))
        return output


databricks_delta_table_destination_entry = DestinationRegistryEntry(
    connection_config=DatabrickDeltaTablesConnectionConfig,
    uploader=DatabrickDeltaTablesUploader,
    uploader_config=DatabrickDeltaTablesUploaderConfig,
    upload_stager=DatabrickDeltaTablesUploadStager,
    upload_stager_config=DatabrickDeltaTablesUploadStagerConfig,
)

