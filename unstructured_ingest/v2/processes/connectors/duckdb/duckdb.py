from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import get_data_df
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
    from duckdb import DuckDBPyConnection as DuckDBConnection

CONNECTOR_TYPE = "duckdb"


class DuckDBAccessConfig(AccessConfig):
    pass


class DuckDBConnectionConfig(ConnectionConfig):
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    database: Optional[str] = Field(
        default=None,
        description="Database name. Path to the DuckDB .db file. If the file does "
        "not exist, it will be created at the specified path.",
    )
    db_schema: Optional[str] = Field(
        default="main",
        description="Schema name. Schema in the database where the elements table is located.",
    )
    table: Optional[str] = Field(
        default="elements",
        description="Table name. Table name into which the elements data is inserted.",
    )
    access_config: Secret[DuckDBAccessConfig] = Field(
        default=DuckDBAccessConfig(), validate_default=True
    )

    def __post_init__(self):
        if self.database is None:
            raise ValueError(
                "A DuckDB connection requires a path to a *.db or *.duckdb file "
                "through the `database` argument"
            )

    @requires_dependencies(["duckdb"], extras="duckdb")
    @contextmanager
    def get_client(self) -> Generator["DuckDBConnection", None, None]:
        import duckdb

        with duckdb.connect(self.database) as client:
            yield client

    @contextmanager
    def get_cursor(self) -> Generator["DuckDBConnection", None, None]:
        with self.get_client() as client:
            with client.cursor() as cursor:
                yield cursor


class DuckDBUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class DuckDBUploadStager(BaseDuckDBUploadStager):
    upload_stager_config: DuckDBUploadStagerConfig = field(
        default_factory=lambda: DuckDBUploadStagerConfig()
    )


class DuckDBUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="[Not-used] Number of records per batch")


@dataclass
class DuckDBUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: DuckDBUploaderConfig
    connection_config: DuckDBConnectionConfig

    def precheck(self) -> None:
        try:
            with self.connection_config.get_cursor() as cursor:
                cursor.execute("SELECT 1;")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def upload_dataframe(self, df: pd.DataFrame) -> None:
        logger.debug(f"uploading {len(df)} entries to {self.connection_config.database} ")

        with self.connection_config.get_client() as conn:
            conn.query(
                f"INSERT INTO {self.connection_config.db_schema}.{self.connection_config.table} BY NAME SELECT * FROM df"  # noqa: E501
            )

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        df = pd.DataFrame(data=data)
        self.upload_dataframe(df=df)

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        df = get_data_df(path)
        self.upload_dataframe(df=df)


duckdb_destination_entry = DestinationRegistryEntry(
    connection_config=DuckDBConnectionConfig,
    uploader=DuckDBUploader,
    uploader_config=DuckDBUploaderConfig,
    upload_stager=DuckDBUploadStager,
    upload_stager_config=DuckDBUploadStagerConfig,
)
