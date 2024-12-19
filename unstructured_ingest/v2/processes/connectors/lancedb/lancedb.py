from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional

import pandas as pd
from pydantic import Field

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.data_prep import flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.constants import RECORD_ID_LABEL
from unstructured_ingest.v2.interfaces.connector import ConnectionConfig
from unstructured_ingest.v2.interfaces.file_data import FileData
from unstructured_ingest.v2.interfaces.upload_stager import UploadStager, UploadStagerConfig
from unstructured_ingest.v2.interfaces.uploader import Uploader, UploaderConfig

CONNECTOR_TYPE = "lancedb"

if TYPE_CHECKING:
    from lancedb import AsyncConnection
    from lancedb.table import AsyncTable


class LanceDBConnectionConfig(ConnectionConfig, ABC):
    uri: str = Field(description="The uri of the database.")

    @abstractmethod
    def get_storage_options(self) -> Optional[dict[str, str]]:
        raise NotImplementedError

    @asynccontextmanager
    @requires_dependencies(["lancedb"], extras="lancedb")
    @DestinationConnectionError.wrap
    async def get_async_connection(self) -> AsyncGenerator["AsyncConnection", None]:
        import lancedb

        with await lancedb.connect_async(
            self.uri,
            storage_options=self.get_storage_options(),
        ) as connection:
            yield connection


class LanceDBRemoteConnectionConfig(LanceDBConnectionConfig):
    timeout: str = Field(
        default="30s",
        description=(
            "Timeout for the entire request, from connection until the response body has finished"
            "in a [0-9]+(ns|us|ms|[smhdwy]) format."
        ),
        pattern=r"[0-9]+(ns|us|ms|[smhdwy])",
    )


class LanceDBUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class LanceDBUploadStager(UploadStager):
    upload_stager_config: LanceDBUploadStagerConfig = field(
        default_factory=LanceDBUploadStagerConfig
    )

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents: list[dict] = json.load(elements_file)

        df = pd.DataFrame(
            [
                self.conform_dict(element_dict=element_dict, file_data=file_data)
                for element_dict in elements_contents
            ]
        )

        output_path = (output_dir / output_filename).with_suffix(".feather")
        df.to_feather(output_path)

        return output_path

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        data = element_dict.copy()
        return {
            "vector": data.pop("embeddings", None),
            RECORD_ID_LABEL: file_data.identifier,
            **flatten_dict(data, separator="-"),
        }


class LanceDBUploaderConfig(UploaderConfig):
    table_name: str = Field(description="The name of the table.")


@dataclass
class LanceDBUploader(Uploader):
    upload_config: LanceDBUploaderConfig
    connection_config: LanceDBConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @DestinationConnectionError.wrap
    def precheck(self):
        async def _precheck() -> None:
            async with self.connection_config.get_async_connection() as conn:
                table = await conn.open_table(self.upload_config.table_name)
                table.close()

        asyncio.run(_precheck())

    @asynccontextmanager
    async def get_table(self) -> AsyncGenerator["AsyncTable", None]:
        async with self.connection_config.get_async_connection() as conn:
            table = await conn.open_table(self.upload_config.table_name)
            try:
                yield table
            finally:
                table.close()

    async def run_async(self, path, file_data, **kwargs):
        df = pd.read_feather(path)
        async with self.get_table() as table:
            schema = await table.schema()
            df = self._fit_to_schema(df, schema)
            if RECORD_ID_LABEL not in schema.names:
                logger.warning(
                    f"Designated table doesn't contain {RECORD_ID_LABEL} column of type"
                    " string which is required to support overwriting updates on subsequent"
                    " uploads of the same record. New rows will be appended instead."
                )
            else:
                await table.delete(f'{RECORD_ID_LABEL} = "{file_data.identifier}"')
            await table.add(data=df)

    def _fit_to_schema(self, df: pd.DataFrame, schema) -> pd.DataFrame:
        columns = set(df.columns)
        schema_fields = set(schema.names)
        columns_to_drop = columns - schema_fields
        missing_columns = schema_fields - columns

        if columns_to_drop:
            logger.info(
                "Following columns will be dropped to match the table's schema: "
                f"{', '.join(columns_to_drop)}"
            )
        if missing_columns:
            logger.info(
                "Following null filled columns will be added to match the table's schema:"
                f" {', '.join(missing_columns)} "
            )

        df = df.drop(columns=columns_to_drop)

        for column in missing_columns:
            df[column] = pd.Series()

        return df
