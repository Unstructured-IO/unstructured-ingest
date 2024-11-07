from __future__ import annotations

import asyncio
import json
from abc import ABC
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator

import pandas as pd
from pydantic import Field

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces.connector import ConnectionConfig
from unstructured_ingest.v2.interfaces.file_data import FileData
from unstructured_ingest.v2.interfaces.upload_stager import UploadStager, UploadStagerConfig
from unstructured_ingest.v2.interfaces.uploader import Uploader, UploaderConfig

CONNECTOR_TYPE = "lancedb"

if TYPE_CHECKING:
    from lancedb import AsyncConnection


class LanceDBConnectionConfig(ConnectionConfig, ABC):
    uri: str = Field(description="The uri of the database.")

    @asynccontextmanager
    @requires_dependencies(["lancedb"], extras="lancedb")
    @DestinationConnectionError.wrap
    async def get_async_connection(self) -> AsyncGenerator["AsyncConnection", None]:
        import lancedb

        connection = await lancedb.connect_async(
            self.uri,
            storage_options=self.access_config.get_secret_value().model_dump(),
        )
        try:
            yield connection
        finally:
            connection.close()


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
                self._conform_element_contents(element_contents)
                for element_contents in elements_contents
            ]
        )

        output_path = (output_dir / output_filename).with_suffix(".feather")
        df.to_feather(output_path)

        return output_path

    def _conform_element_contents(self, element: dict) -> dict:
        metadata = element.pop("metadata")
        data_sources = metadata.pop("data_sources", {})
        coordinates = metadata.pop("coordinates", {})
        return {
            "vector": element.pop("embeddings", None),
            **metadata,
            **data_sources,
            **coordinates,
            **element,
        }


class LanceDBUploaderConfig(UploaderConfig):
    table_name: str = Field(description="The name of the table.")


@dataclass
class LanceDBUploader(Uploader):
    upload_config: LanceDBUploaderConfig
    connection_config: LanceDBConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    async def run_async(self, path, file_data, **kwargs):
        df = pd.read_feather(path)

        async with self.connection_config.get_async_connection() as conn:
            table = await conn.open_table(self.upload_config.table_name)
            schema = await table.schema()
            df = self._fit_to_schema(df, schema)
            await table.add(data=df)
            table.close()

    def _fit_to_schema(self, df: pd.DataFrame, schema) -> pd.DataFrame:
        columns = set(df.columns)
        schema_fields = set(schema.names)
        columns_to_drop = columns - schema_fields
        missing_columns = schema_fields - columns

        df = df.drop(columns=columns_to_drop)

        for column in missing_columns:
            df[column] = pd.Series()

        return df

    @DestinationConnectionError.wrap
    def precheck(self):
        async def _precheck() -> None:
            async with self.connection_config.get_async_connection() as conn:
                table = await conn.open_table(self.upload_config.table_name)
                table.close()

        asyncio.run(_precheck())
