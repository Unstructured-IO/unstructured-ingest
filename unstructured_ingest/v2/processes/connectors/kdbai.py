import json
import uuid
from dataclasses import field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    UploadContent,
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
    from kdbai_client import Session, Table


class KdbaiAccessConfig(AccessConfig):
    api_key: str = Field()


class KdbaiConnectionConfig(ConnectionConfig):
    access_config: Secret[KdbaiAccessConfig]
    endpoint: str

    @requires_dependencies(["kdbai_client"], extras="kdbai")
    def get_session(self) -> "Session":
        from kdbai_client import Session

        return Session(
            api_key=self.access_config.get_secret_value().api_key, endpoint=self.endpoint
        )


class KdbaiUploadStagerConfig(UploadStagerConfig):
    pass


class KdbaiUploadStager(UploadStager):
    upload_stager_config: KdbaiUploadStagerConfig = field(default_factory=KdbaiUploadStagerConfig)

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

        data = []
        for element in elements_contents:
            data.append(
                {
                    "id": str(uuid.uuid4()),
                    "element_id": element.get("element_id"),
                    "document": element.pop("text", None),
                    "embedding": element.get("embedding"),
                    "metadata": flatten_dict(
                        dictionary=element.get("metadata"),
                        separator="-",
                        flatten_lists=True,
                        remove_none=True,
                    ),
                }
            )
        df = pd.DataFrame(data)
        with output_path.open("w") as output_file:
            df.to_csv(output_file, index=False)
        return output_path


class KdbaiUploaderConfig(UploaderConfig):
    table_name: str
    batch_size: int = 100


class KdbaiUploader(Uploader):
    connection_config: KdbaiConnectionConfig
    upload_config: KdbaiUploaderConfig

    def precheck(self) -> None:
        try:
            self.get_table()
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def get_table(self) -> "Table":
        session: Session = self.connection_config.get_session()
        table = session.table(self.upload_config.table_name)
        return table

    def upsert_batch(self, batch: pd.DataFrame):
        table = self.get_table()
        table.insert(data=batch)

    def run(self, contents: list[UploadContent], **kwargs: Any) -> None:
        df = pd.concat((pd.read_csv(content.path) for content in contents), ignore_index=True)
        for _, batch_df in df.groupby(np.arange(len(df)) // self.upload_config.batch_size):
            self.upsert_batch(batch=batch_df)


kdbai_destination_entry = DestinationRegistryEntry(
    connection_config=KdbaiConnectionConfig,
    uploader=KdbaiUploader,
    uploader_config=KdbaiUploaderConfig,
    upload_stager=KdbaiUploadStager,
    upload_stager_config=KdbaiUploadStagerConfig,
)
