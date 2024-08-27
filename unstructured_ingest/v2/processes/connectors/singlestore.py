import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import numpy as np
import pandas as pd
from dateutil import parser
from pydantic import Field, Secret

from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.table import convert_to_pandas_dataframe
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
)

if TYPE_CHECKING:
    from singlestoredb.connection import Connection

CONNECTOR_TYPE = "singlestore"


class SingleStoreAccessConfig(AccessConfig):
    password: Optional[str] = Field(default=None, description="SingleStore password")


class SingleStoreConnectionConfig(ConnectionConfig):
    host: Optional[str] = Field(default=None, description="SingleStore host")
    port: Optional[int] = Field(default=None, description="SingleStore port")
    user: Optional[str] = Field(default=None, description="SingleStore user")
    database: Optional[str] = Field(default=None, description="SingleStore database")
    access_config: Secret[SingleStoreAccessConfig]

    @requires_dependencies(["singlestoredb"], extras="singlestore")
    def get_connection(self) -> "Connection":
        import singlestoredb as s2

        conn = s2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.access_config.get_secret_value().password,
        )
        return conn


class SingleStoreUploadStagerConfig(UploadStagerConfig):
    drop_empty_cols: bool = Field(default=False, description="Drop any columns that have no data")


@dataclass
class SingleStoreUploadStager(UploadStager):
    upload_stager_config: SingleStoreUploadStagerConfig

    @staticmethod
    def parse_date_string(date_string: str) -> date:
        try:
            timestamp = float(date_string)
            return datetime.fromtimestamp(timestamp)
        except Exception as e:
            logger.debug(f"date {date_string} string not a timestamp: {e}")
        return parser.parse(date_string)

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)
        output_path = Path(output_dir) / Path(f"{output_filename}.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df = convert_to_pandas_dataframe(
            elements_dict=elements_contents,
            drop_empty_cols=self.upload_stager_config.drop_empty_cols,
        )
        datetime_columns = [
            "data_source_date_created",
            "data_source_date_modified",
            "data_source_date_processed",
        ]
        for column in filter(lambda x: x in df.columns, datetime_columns):
            df[column] = df[column].apply(self.parse_date_string)
        if "data_source_record_locator" in df.columns:
            df["data_source_record_locator"] = df["data_source_record_locator"].apply(
                lambda x: json.dumps(x) if x else None
            )

        with output_path.open("w") as output_file:
            df.to_csv(output_file, index=False)
        return output_path


class SingleStoreUploaderConfig(UploaderConfig):
    table_name: str = Field(description="SingleStore table to write contents to")
    batch_size: int = Field(default=100, description="Batch size when writing to SingleStore")


@dataclass
class SingleStoreUploader(Uploader):
    connection_config: SingleStoreConnectionConfig
    upload_config: SingleStoreUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def upload_csv(self, csv_path: Path) -> None:
        df = pd.read_csv(csv_path)
        logger.debug(
            f"uploading {len(df)} entries to {self.connection_config.database} "
            f"db in table {self.upload_config.table_name}"
        )
        stmt = "INSERT INTO {} ({}) VALUES ({})".format(
            self.upload_config.table_name,
            ", ".join(df.columns),
            ", ".join(["%s"] * len(df.columns)),
        )
        logger.debug(f"sql statement: {stmt}")
        df.replace({np.nan: None}, inplace=True)
        data_as_tuples = list(df.itertuples(index=False, name=None))
        with self.connection_config.get_connection() as conn:
            with conn.cursor() as cur:
                for chunk in batch_generator(
                    data_as_tuples, batch_size=self.upload_config.batch_size
                ):
                    cur.executemany(stmt, chunk)
                    conn.commit()

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        if path.suffix != ".csv":
            raise ValueError(f"Only .csv files are supported: {path}")
        self.upload_csv(csv_path=path)


singlestore_destination_entry = DestinationRegistryEntry(
    connection_config=SingleStoreConnectionConfig,
    uploader=SingleStoreUploader,
    uploader_config=SingleStoreUploaderConfig,
    upload_stager=SingleStoreUploadStager,
    upload_stager_config=SingleStoreUploadStagerConfig,
)
