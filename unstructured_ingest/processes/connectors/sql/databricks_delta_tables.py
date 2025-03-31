import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
)
from unstructured_ingest.processes.connectors.sql.sql import (
    SQLAccessConfig,
    SQLConnectionConfig,
    SQLUploader,
    SQLUploaderConfig,
    SQLUploadStager,
    SQLUploadStagerConfig,
)
from unstructured_ingest.utils.data_prep import split_dataframe
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from databricks.sdk.core import oauth_service_principal
    from databricks.sql.client import Connection as DeltaTableConnection
    from databricks.sql.client import Cursor as DeltaTableCursor
    from pandas import DataFrame

CONNECTOR_TYPE = "databricks_delta_tables"


class DatabricksDeltaTablesAccessConfig(SQLAccessConfig):
    token: Optional[str] = Field(default=None, description="Databricks Personal Access Token")
    client_id: Optional[str] = Field(default=None, description="Client ID of the OAuth app.")
    client_secret: Optional[str] = Field(
        default=None, description="Client Secret of the OAuth app."
    )


class DatabricksDeltaTablesConnectionConfig(SQLConnectionConfig):
    access_config: Secret[DatabricksDeltaTablesAccessConfig]
    server_hostname: str = Field(description="server hostname connection config value")
    http_path: str = Field(description="http path connection config value")

    @requires_dependencies(["databricks"], extras="databricks-delta-tables")
    def get_credentials_provider(self) -> "oauth_service_principal":
        from databricks.sdk.core import Config, oauth_service_principal

        host = f"https://{self.server_hostname}"
        access_configs = self.access_config.get_secret_value()
        client_id = access_configs.client_id
        client_secret = access_configs.client_secret

        def _get_credentials_provider():
            return oauth_service_principal(
                Config(
                    host=host,
                    client_id=client_id,
                    client_secret=client_secret,
                )
            )

        if client_id and client_secret:
            return _get_credentials_provider

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
    def get_connection(self, **connect_kwargs) -> Generator["DeltaTableConnection", None, None]:
        from databricks.sql import connect

        connect_kwargs = connect_kwargs or {}
        connect_kwargs["_user_agent_entry"] = os.getenv(
            "UNSTRUCTURED_USER_AGENT", "unstructuredio_oss"
        )
        connect_kwargs["server_hostname"] = connect_kwargs.get(
            "server_hostname", self.server_hostname
        )
        connect_kwargs["http_path"] = connect_kwargs.get("http_path", self.http_path)

        if credential_provider := self.get_credentials_provider():
            connect_kwargs["credentials_provider"] = credential_provider
        else:
            connect_kwargs["access_token"] = self.access_config.get_secret_value().token
        with connect(**connect_kwargs) as connection:
            yield connection

    @contextmanager
    def get_cursor(self, **connect_kwargs) -> Generator["DeltaTableCursor", None, None]:
        with self.get_connection(**connect_kwargs) as connection:
            cursor = connection.cursor()
            yield cursor


class DatabricksDeltaTablesUploadStagerConfig(SQLUploadStagerConfig):
    pass


class DatabricksDeltaTablesUploadStager(SQLUploadStager):
    upload_stager_config: DatabricksDeltaTablesUploadStagerConfig


class DatabricksDeltaTablesUploaderConfig(SQLUploaderConfig):
    catalog: str = Field(description="Name of the catalog in the Databricks Unity Catalog service")
    database: str = Field(description="Database name", default="default")
    table_name: str = Field(description="Table name")


@dataclass
class DatabricksDeltaTablesUploader(SQLUploader):
    upload_config: DatabricksDeltaTablesUploaderConfig
    connection_config: DatabricksDeltaTablesConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["pandas"], extras="databricks-delta-tables")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        super().run(path=path, file_data=file_data, **kwargs)

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

    def create_statement(self, columns: list[str], values: tuple[Any, ...]) -> str:
        values_list = []
        for v in values:
            if isinstance(v, dict):
                values_list.append(json.dumps(v))
            elif isinstance(v, list):
                if v and isinstance(v[0], (int, float)):
                    values_list.append("ARRAY({})".format(", ".join([str(val) for val in v])))
                else:
                    values_list.append("ARRAY({})".format(", ".join([f"'{val}'" for val in v])))
            else:
                values_list.append(f"'{v}'")
        statement = "INSERT INTO {table_name} ({columns}) VALUES({values})".format(
            table_name=self.upload_config.table_name,
            columns=", ".join(columns),
            values=", ".join(values_list),
        )
        return statement

    @requires_dependencies(["pandas"], extras="databricks-delta-tables")
    def upload_dataframe(self, df: "DataFrame", file_data: FileData) -> None:
        import numpy as np

        if self.can_delete():
            self.delete_by_record_id(file_data=file_data)
        else:
            logger.warning(
                f"table doesn't contain expected "
                f"record id column "
                f"{self.upload_config.record_id_key}, skipping delete"
            )
        df.replace({np.nan: None}, inplace=True)
        self._fit_to_schema(df=df)

        columns = list(df.columns)
        logger.info(
            f"writing a total of {len(df)} elements via"
            f" document batches to destination"
            f" table named {self.upload_config.table_name}"
            # f" with batch size {self.upload_config.batch_size}"
        )
        # TODO: currently variable binding not supporting for list data_types,
        #  update once that gets resolved in SDK
        for rows in split_dataframe(df=df, chunk_size=self.upload_config.batch_size):
            with self.get_cursor() as cursor:
                values = self.prepare_data(columns, tuple(rows.itertuples(index=False, name=None)))
                for v in values:
                    stmt = self.create_statement(columns=columns, values=v)
                    cursor.execute(stmt)


databricks_delta_tables_destination_entry = DestinationRegistryEntry(
    connection_config=DatabricksDeltaTablesConnectionConfig,
    uploader=DatabricksDeltaTablesUploader,
    uploader_config=DatabricksDeltaTablesUploaderConfig,
    upload_stager=DatabricksDeltaTablesUploadStager,
    upload_stager_config=DatabricksDeltaTablesUploadStagerConfig,
)
