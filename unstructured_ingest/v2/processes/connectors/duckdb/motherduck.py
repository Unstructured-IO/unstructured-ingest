from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.__version__ import __version__ as unstructured_io_ingest_version
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
    UploadStagerConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.v2.processes.connectors.duckdb.base import BaseDuckDBUploadStager

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection as MotherDuckConnection

CONNECTOR_TYPE = "motherduck"


class MotherDuckAccessConfig(AccessConfig):
    md_token: Optional[str] = Field(default=None, description="MotherDuck token")


class MotherDuckConnectionConfig(ConnectionConfig):
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    database: Optional[str] = Field(
        default=None,
        description="Database name. This is the name of the MotherDuck database.",
    )
    db_schema: Optional[str] = Field(
        default="main",
        description="Schema name. This is the schema within the database where the elements table is located.",
    )
    table: Optional[str] = Field(
        default="elements",
        description="Table name. This is the table name into which the elements data is inserted.",
    )
    access_config: Secret[MotherDuckAccessConfig] = Field(
        default=MotherDuckAccessConfig(), validate_default=True
    )

    def __post_init__(self):
        if self.database is None:
            raise ValueError(
                "A MotherDuck connection requires a database (string) to be passed "
                "through the `database` argument"
            )
        if self.access_config.md_token is None:
            raise ValueError(
                "A MotherDuck connection requires a md_token (MotherDuck token) to be passed "
                "using MotherDuckAccessConfig through the `access_config` argument"
            )


class MotherDuckUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class MotherDuckUploadStager(BaseDuckDBUploadStager):
    upload_stager_config: MotherDuckUploadStagerConfig = field(
        default_factory=lambda: MotherDuckUploadStagerConfig()
    )


class MotherDuckUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="[Not-used] Number of records per batch")


@dataclass
class MotherDuckUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: MotherDuckUploaderConfig
    connection_config: MotherDuckConnectionConfig

    def precheck(self) -> None:
        try:
            cursor = self.connection().cursor()
            cursor.execute("SELECT 1;")
            cursor.close()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @property
    def connection(self) -> Callable[[], "MotherDuckConnection"]:
        return self._make_motherduck_connection

    @requires_dependencies(["duckdb"], extras="duckdb")
    def _make_motherduck_connection(self) -> "MotherDuckConnection":
        import duckdb

        access_config = self.connection_config.access_config.get_secret_value()
        conn = duckdb.connect(
            f"md:?motherduck_token={access_config.md_token}",
            config={
                "custom_user_agent": f"unstructured-io-ingest/{unstructured_io_ingest_version}"
            },
        )

        conn.sql(f"USE {self.connection_config.database}")

        return conn

    def upload_contents(self, path: Path) -> None:
        df_elements = pd.read_json(path, orient="records", lines=True)
        logger.debug(f"uploading {len(df_elements)} entries to {self.connection_config.database} ")

        with self.connection() as conn:
            conn.query(
                f"INSERT INTO {self.connection_config.db_schema}.{self.connection_config.table} BY NAME SELECT * FROM df_elements"
            )

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        self.upload_contents(path=path)


motherduck_destination_entry = DestinationRegistryEntry(
    connection_config=MotherDuckConnectionConfig,
    uploader=MotherDuckUploader,
    uploader_config=MotherDuckUploaderConfig,
    upload_stager=MotherDuckUploadStager,
    upload_stager_config=MotherDuckUploadStagerConfig,
)
