from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.__version__ import __version__ as unstructured_io_ingest_version
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
    from duckdb import DuckDBPyConnection as MotherDuckConnection

CONNECTOR_TYPE = "motherduck"


class MotherDuckAccessConfig(AccessConfig):
    md_token: str = Field(default=None, description="MotherDuck token")


class MotherDuckConnectionConfig(ConnectionConfig):
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    database: str = Field(
        description="Database name. Name of the MotherDuck database.",
    )
    db_schema: Optional[str] = Field(
        default="main",
        description="Schema name. Schema in the database where the elements table is located.",
    )
    table: Optional[str] = Field(
        default="elements",
        description="Table name. Table name into which the elements data is inserted.",
    )
    access_config: Secret[MotherDuckAccessConfig] = Field(
        default=MotherDuckAccessConfig(), validate_default=True
    )

    @requires_dependencies(["duckdb"], extras="duckdb")
    @contextmanager
    def get_client(self) -> Generator["MotherDuckConnection", None, None]:
        import duckdb

        access_config = self.access_config.get_secret_value()
        with duckdb.connect(
            f"md:?motherduck_token={access_config.md_token}",
            config={
                "custom_user_agent": f"unstructured-io-ingest/{unstructured_io_ingest_version}"
            },
        ) as conn:
            conn.sql(f"USE {self.database}")
            yield conn

    @contextmanager
    def get_cursor(self) -> Generator["MotherDuckConnection", None, None]:
        with self.get_client() as client:
            with client.cursor() as cursor:
                yield cursor


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


motherduck_destination_entry = DestinationRegistryEntry(
    connection_config=MotherDuckConnectionConfig,
    uploader=MotherDuckUploader,
    uploader_config=MotherDuckUploaderConfig,
    upload_stager=MotherDuckUploadStager,
    upload_stager_config=MotherDuckUploadStagerConfig,
)
