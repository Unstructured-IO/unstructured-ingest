import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.v2.processes.connectors.sql.sql import (
    _DATE_COLUMNS,
    SQLAccessConfig,
    SQLConnectionConfig,
    SQLUploader,
    SQLUploaderConfig,
    SQLUploadStager,
    SQLUploadStagerConfig,
    parse_date_string,
)

if TYPE_CHECKING:
    from sqlite3 import Connection as SqliteConnection

CONNECTOR_TYPE = "sqlite"


class SQLiteAccessConfig(SQLAccessConfig):
    pass


class SQLiteConnectionConfig(SQLConnectionConfig):
    access_config: Secret[SQLiteAccessConfig] = Field(
        default=SQLiteAccessConfig(), validate_default=True
    )
    database_path: Path = Field(
        description="Path to the .db file.",
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    def get_connection(self) -> "SqliteConnection":
        from sqlite3 import connect

        return connect(database=self.database_path)


class SQLiteUploadStagerConfig(SQLUploadStagerConfig):
    pass


class SQLiteUploadStager(SQLUploadStager):
    upload_stager_config: SQLiteUploadStagerConfig


class SQLiteUploaderConfig(SQLUploaderConfig):
    pass


@dataclass
class SQLiteUploader(SQLUploader):
    upload_config: SQLiteUploaderConfig = field(default_factory=SQLiteUploaderConfig)
    connection_config: SQLiteConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def prepare_data(
        self, columns: list[str], data: tuple[tuple[Any, ...], ...]
    ) -> list[tuple[Any, ...]]:
        output = []
        for row in data:
            parsed = []
            for column_name, value in zip(columns, row):
                if isinstance(value, (list, dict)):
                    value = json.dumps(value)
                if column_name in _DATE_COLUMNS:
                    if value is None:
                        parsed.append(None)
                    else:
                        parsed.append(parse_date_string(value))
                else:
                    parsed.append(value)
            output.append(tuple(parsed))
        return output

    def upload_contents(self, path: Path) -> None:
        df = pd.read_json(path, orient="records", lines=True)
        logger.debug(f"uploading {len(df)} entries to {self.connection_config.database_path} ")
        df.replace({np.nan: None}, inplace=True)

        columns = tuple(df.columns)
        stmt = f"INSERT INTO {self.upload_config.table_name} ({','.join(columns)}) \
                VALUES({','.join(['?' for x in columns])})"  # noqa E501

        for rows in pd.read_json(
            path, orient="records", lines=True, chunksize=self.upload_config.batch_size
        ):
            with self.connection_config.get_connection() as conn:
                values = self.prepare_data(columns, tuple(rows.itertuples(index=False, name=None)))
                conn.executemany(stmt, values)
                conn.commit()


sqlite_destination_entry = DestinationRegistryEntry(
    connection_config=SQLiteConnectionConfig,
    uploader=SQLiteUploader,
    uploader_config=SQLiteUploaderConfig,
    upload_stager=SQLiteUploadStager,
    upload_stager_config=SQLiteUploadStagerConfig,
)
