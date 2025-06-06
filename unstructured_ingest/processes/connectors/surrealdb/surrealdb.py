import os

from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Uploader,
    UploaderConfig,
    UploadStagerConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.processes.connectors.surrealdb.base import BaseSurrealDBUploadStager
from unstructured_ingest.utils.data_prep import get_data_df
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from surrealdb import Surreal as SurrealDBConnection
    from pandas import DataFrame

CONNECTOR_TYPE = "surrealdb"


def normalize_url(url: str) -> str:
    """
    Get a normalized version of the destination url.
    Translate rocksdb:NAME, surrealkv:NAME, and file:NAME to rocksdb://NAME, surrealkv://NAME, and file://NAME respectively.
    """
    if "://" not in url:
        components = url.split(":")
        if len(components) == 2:
            return f"{components[0]}://{components[1]}"
        else:
            raise ValueError(f"Invalid URL: {url}")

    return url


class SurrealDBAccessConfig(AccessConfig):
    username: Optional[str] = Field(
        default=None,
        description="Username for signing into the SurrealdB instance.",
    )
    password: Optional[str] = Field(
        default=None,
        description="Password for signing into the SurrealdB instance.",
    )
    token: Optional[str] = Field(
        default=None,
        description="Token for authentication into the SurrealdB instance.",
    )


class SurrealDBConnectionConfig(ConnectionConfig):
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    url: str = Field(
        description="URL of the SurrealdB instance.",
    )
    namespace: str = Field(
        description="Namespace name. Namespace in the SurrealdB instance where the database is located.",
    )
    database: str = Field(
        description="Database name. Table is created in this database.",
    )
    table: Optional[str] = Field(
        default="elements",
        description="Table name. Table name into which the elements data is inserted.",
    )
    access_config: Secret[SurrealDBAccessConfig] = Field(
        default=SurrealDBAccessConfig(), validate_default=True
    )

    def __post_init__(self):
        if self.url is None:
            raise ValueError(
                "A SurrealDB connection requires a url to be specified through the `url` argument"
            )
        self.url = normalize_url(self.url)
        if self.namespace is None:
            raise ValueError(
                "A SurrealDB connection requires a namespace to be specified "
                "through the `namespace` argument"
            )
        if self.database is None:
            raise ValueError(
                "A SurrealDB connection requires a database to be specified "
                "through the `database` argument"
            )
        access_config = self.access_config.get_secret_value()
        if (
            access_config.username is None
            and access_config.password is None
            and access_config.token is None
        ):
            raise ValueError(
                "A SurrealDB connection requires a username, password, or token to be specified "
                "through the `access_config` argument"
            )

    @requires_dependencies(["surrealdb"], extras="surrealdb")
    @contextmanager
    def get_client(self) -> Generator["SurrealDBConnection", None, None]:
        import surrealdb

        url = normalize_url(self.url)
        if url.startswith("surrealkv:") or url.startswith("rocksdb:") or url.startswith("file:"):
            components = url.split("://")
            logger.info("Using %s at %s", components[0], components[1])
            os.makedirs(os.path.dirname(components[1]), exist_ok=True)

        signin_args = {}
        access_config = self.access_config.get_secret_value()
        if access_config.token is not None:
            signin_args["token"] = str(access_config.token)
        if access_config.username is not None:
            signin_args["username"] = str(access_config.username)
        if access_config.password is not None:
            signin_args["password"] = str(access_config.password)

        with surrealdb.Surreal(url) as client:
            if signin_args.keys().__len__() > 0:
                client.signin(signin_args)
            client.use(self.namespace, self.database)
            yield client


class SurrealDBUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class SurrealDBUploadStager(BaseSurrealDBUploadStager):
    upload_stager_config: SurrealDBUploadStagerConfig = field(
        default_factory=lambda: SurrealDBUploadStagerConfig()
    )


class SurrealDBUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="[Not-used] Number of records per batch")


@dataclass
class SurrealDBUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: SurrealDBUploaderConfig
    connection_config: SurrealDBConnectionConfig

    def precheck(self) -> None:
        try:
            with self.connection_config.get_client() as conn:
                conn.query("SELECT 1;")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def upload_dataframe(self, df: "DataFrame") -> None:
        logger.debug(f"uploading {len(df)} entries to {self.connection_config.table} ")

        with self.connection_config.get_client() as conn:
            columns = df.columns.tolist()

            for _, row in df.iterrows():
                row_dict = {col: row[col] for col in columns}
                logger.debug("inserting row: %s", row_dict)
                conn.insert(self.connection_config.table, row_dict)

    @requires_dependencies(["pandas"], extras="surrealdb")
    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        import pandas as pd

        df = pd.DataFrame(data=data)
        self.upload_dataframe(df=df)

    @requires_dependencies(["pandas"], extras="surrealdb")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        df = get_data_df(path)
        self.upload_dataframe(df=df)


surrealdb_destination_entry = DestinationRegistryEntry(
    connection_config=SurrealDBConnectionConfig,
    uploader=SurrealDBUploader,
    uploader_config=SurrealDBUploaderConfig,
    upload_stager=SurrealDBUploadStager,
    upload_stager_config=SurrealDBUploadStagerConfig,
)
