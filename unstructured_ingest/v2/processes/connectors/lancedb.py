import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces.connector import AccessConfig, ConnectionConfig
from unstructured_ingest.v2.interfaces.file_data import FileData
from unstructured_ingest.v2.interfaces.upload_stager import UploadStager, UploadStagerConfig
from unstructured_ingest.v2.interfaces.uploader import Uploader, UploaderConfig
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

CONNECTOR_TYPE = "lancedb"

if TYPE_CHECKING:
    from lancedb import AsyncConnection


SUPPORTED_ELEMENT_FIELDS = (
    "text",
    "element_id",
    "type",
)
SUPPORTED_ELEMENT_METADATA_FIELDS = (
    "filename",
    "is_continuation",
    "file_type",
    "page_number",
    "text_as_html",
)


class LanceDBAccessConfig(AccessConfig):
    s3_access_key_id: Optional[str] = Field(default=None)
    s3_secret_access_key: Optional[str] = Field(default=None)
    google_service_account_key: Optional[str] = Field(default=None)
    azure_storage_account_name: Optional[str] = Field(default=None)
    azure_storage_account_key: Optional[str] = Field(default=None)

    @property
    def storage_options(self) -> dict:
        return {
            "aws_access_key_id": self.s3_access_key_id,
            "aws_secret_access_key": self.s3_secret_access_key,
            "google_service_account_key": self.google_service_account_key,
            "azure_storage_account_name": self.azure_storage_account_name,
            "azure_storage_account_key": self.azure_storage_account_key,
        }


class LanceDBConnectionConfig(ConnectionConfig):
    access_config: Secret[LanceDBAccessConfig] = Field(
        default_factory=LanceDBAccessConfig, validate_default=True
    )
    uri: str
    table_name: str

    @requires_dependencies(["lancedb"], extras="lancedb")
    @DestinationConnectionError.wrap
    async def get_async_connection(self) -> "AsyncConnection":
        import lancedb

        return await lancedb.connect_async(
            self.uri,
            storage_options=self.access_config.get_secret_value().storage_options,
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
                self._conform_element_contents(element_contents)
                for element_contents in elements_contents
            ]
        )

        output_path = (output_dir / output_filename).with_suffix(".feather")
        df.to_feather(output_path)

        return output_path

    def _conform_element_contents(self, element: dict) -> dict:
        conformed_contents = {"vector": element.get("embeddings")}
        for field in SUPPORTED_ELEMENT_FIELDS:
            conformed_contents[field] = element.get(field)
        for field in SUPPORTED_ELEMENT_METADATA_FIELDS:
            conformed_contents[field] = element.get("metadata", {}).get(field)

        return conformed_contents


class LanceDBUploaderConfig(UploaderConfig):
    pass


@dataclass
class LanceDBUploader(Uploader):
    upload_config: LanceDBUploaderConfig
    connection_config: LanceDBConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    async def run_async(self, path, file_data, **kwargs):
        async_connection = await self.connection_config.get_async_connection()
        df = pd.read_feather(path)

        table = await async_connection.open_table(self.connection_config.table_name)
        await table.add(data=df)


lancedb_table_destination_entry = DestinationRegistryEntry(
    connection_config=LanceDBConnectionConfig,
    uploader=LanceDBUploader,
    uploader_config=LanceDBUploaderConfig,
    upload_stager=LanceDBUploadStager,
    upload_stager_config=LanceDBUploadStagerConfig,
)
