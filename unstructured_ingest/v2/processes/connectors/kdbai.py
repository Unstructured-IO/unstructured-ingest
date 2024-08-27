import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

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

CONNECTOR_TYPE = "kdbai"


class KdbaiAccessConfig(AccessConfig):
    api_key: Optional[str] = Field(
        default=None,
        description="A string for the api-key, can be left empty "
        "when connecting to local KDBAI instance.",
    )


SecretKdbaiAccessConfig = Secret[KdbaiAccessConfig]


class KdbaiConnectionConfig(ConnectionConfig):
    access_config: SecretKdbaiAccessConfig = Field(
        default=SecretKdbaiAccessConfig(secret_value=KdbaiAccessConfig())
    )
    endpoint: str = Field(
        default="http://localhost:8082", description="Endpoint url where KDBAI is hosted."
    )

    @requires_dependencies(["kdbai_client"], extras="kdbai")
    def get_session(self) -> "Session":
        from kdbai_client import Session

        return Session(
            api_key=self.access_config.get_secret_value().api_key, endpoint=self.endpoint
        )


class KdbaiUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
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
        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        data = []
        for element in elements_contents:
            data.append(
                {
                    "id": str(uuid.uuid4()),
                    "element_id": element.get("element_id"),
                    "document": element.pop("text", None),
                    "embeddings": element.get("embeddings"),
                    "metadata": flatten_dict(
                        dictionary=element.get("metadata"),
                        flatten_lists=True,
                        remove_none=True,
                    ),
                }
            )
        logger.debug(f"writing {len(data)} elements to {output_path}")
        with output_path.open("w") as output_file:
            json.dump(data, output_file, indent=2)
        return output_path


class KdbaiUploaderConfig(UploaderConfig):
    table_name: str = Field(description="The name of the KDBAI table to write into.")
    batch_size: int = Field(default=100, description="Number of records per batch")


@dataclass
class KdbaiUploader(Uploader):
    connection_config: KdbaiConnectionConfig
    upload_config: KdbaiUploaderConfig
    connector_type: str = field(default=CONNECTOR_TYPE, init=False)

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

    def process_dataframe(self, df: pd.DataFrame):
        logger.debug(
            f"uploading {len(df)} entries to {self.connection_config.endpoint} "
            f"db in table {self.upload_config.table_name}"
        )
        for _, batch_df in df.groupby(np.arange(len(df)) // self.upload_config.batch_size):
            self.upsert_batch(batch=batch_df)

    def process_csv(self, csv_paths: list[Path]):
        logger.debug(f"uploading content from {len(csv_paths)} csv files")
        df = pd.concat((pd.read_csv(path) for path in csv_paths), ignore_index=True)
        self.process_dataframe(df=df)

    def process_json(self, json_paths: list[Path]):
        logger.debug(f"uploading content from {len(json_paths)} json files")
        all_records = []
        for p in json_paths:
            with open(p) as json_file:
                all_records.extend(json.load(json_file))

        df = pd.DataFrame(data=all_records)
        self.process_dataframe(df=df)

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        if path.suffix == ".csv":
            self.process_csv(csv_paths=[path])
        elif path.suffix == ".json":
            self.process_json(json_paths=[path])
        else:
            raise ValueError(f"Unsupported file type, must be json or csv file: {path}")


kdbai_destination_entry = DestinationRegistryEntry(
    connection_config=KdbaiConnectionConfig,
    uploader=KdbaiUploader,
    uploader_config=KdbaiUploaderConfig,
    upload_stager=KdbaiUploadStager,
    upload_stager_config=KdbaiUploadStagerConfig,
)
