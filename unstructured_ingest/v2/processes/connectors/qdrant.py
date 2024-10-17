import json
import multiprocessing as mp
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from dateutil import parser
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError, WriteError
from unstructured_ingest.utils.data_prep import batch_generator, flatten_dict
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
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

if TYPE_CHECKING:
    from qdrant_client import QdrantClient


CONNECTOR_TYPE = "qdrant"


class QdrantAccessConfig(AccessConfig):
    api_key: Optional[str] = Field(default=None, description="API Key")


class QdrantConnectionConfig(ConnectionConfig):
    collection_name: str = Field(description="API Key")
    location: Optional[str] = Field(default=None, description="Location")
    url: Optional[str] = Field(default=None, description="URL")
    port: Optional[int] = Field(default=6333, description="Port")
    grpc_port: Optional[int] = Field(default=6334, description="GRPC Port")
    prefer_grpc: Optional[bool] = Field(default=False, description="Prefer GRCP")
    https: Optional[bool] = Field(default=None, description="HTTPS")
    prefix: Optional[str] = Field(default=None, description="Prefix")
    timeout: Optional[float] = Field(default=None, description="TimeOut time")
    host: Optional[str] = Field(default=None, description="Host")
    path: Optional[str] = Field(default=None, description="Path")
    force_disable_check_same_thread: Optional[bool] = Field(
        default=False, description="Force disable check for same thread"
    )
    access_config: Secret[QdrantAccessConfig] = Field(default=None, description="Access Config")


class QdrantUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class QdrantUploadStager(UploadStager):
    upload_stager_config: QdrantUploadStagerConfig = field(
        default_factory=lambda: QdrantUploadStagerConfig()
    )

    @staticmethod
    def parse_date_string(date_string: str) -> date:
        try:
            timestamp = float(date_string)
            return datetime.fromtimestamp(timestamp)
        except Exception as e:
            logger.debug(f"date {date_string} string not a timestamp: {e}")
        return parser.parse(date_string)

    @staticmethod
    def conform_dict(data: dict) -> dict:
        """
        Prepares dictionary in the format that Chroma requires
        """
        return {
            "id": str(uuid.uuid4()),
            "vector": data.pop("embeddings", {}),
            "payload": {
                "text": data.pop("text", None),
                "element_serialized": json.dumps(data),
                **flatten_dict(
                    data,
                    separator="-",
                    flatten_lists=True,
                ),
            },
        }

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
        conformed_elements = [self.conform_dict(data=element) for element in elements_contents]
        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        with open(output_path, "w") as output_file:
            json.dump(conformed_elements, output_file)
        return output_path


class QdrantUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="Number of records per batch")
    num_processes: Optional[int] = Field(
        default=1, description="Optional limit on number of threads to use for upload"
    )


@dataclass
class QdrantUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: QdrantUploaderConfig
    connection_config: QdrantConnectionConfig
    _client: Optional["QdrantClient"] = None

    @property
    def qdrant_client(self):
        if self._client is None:
            self._client = self.create_client()
        return self._client

    @requires_dependencies(["qdrant_client"], extras="qdrant")
    def create_client(self) -> "QdrantClient":
        from qdrant_client import QdrantClient

        client = QdrantClient(
            location=self.connection_config.location,
            url=self.connection_config.url,
            port=self.connection_config.port,
            grpc_port=self.connection_config.grpc_port,
            prefer_grpc=self.connection_config.prefer_grpc,
            https=self.connection_config.https,
            api_key=(
                self.connection_config.access_config.api_key
                if self.connection_config.access_config
                else None
            ),
            prefix=self.connection_config.prefix,
            timeout=self.connection_config.timeout,
            host=self.connection_config.host,
            path=self.connection_config.path,
            force_disable_check_same_thread=self.connection_config.force_disable_check_same_thread,
        )

        return client

    @DestinationConnectionError.wrap
    def check_connection(self):
        self.qdrant_client.get_collections()

    def precheck(self) -> None:
        try:
            self.check_connection()
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @DestinationConnectionError.wrap
    @requires_dependencies(["qdrant_client"], extras="qdrant")
    def upsert_batch(self, batch: List[Dict[str, Any]]):
        from qdrant_client import models

        client = self.qdrant_client
        try:
            points: list[models.PointStruct] = [models.PointStruct(**item) for item in batch]
            response = client.upsert(
                self.connection_config.collection_name, points=points, wait=True
            )
        except Exception as api_error:
            raise WriteError(f"Qdrant error: {api_error}") from api_error
        logger.debug(f"results: {response}")

    def write_dict(self, *args, elements_dict: List[Dict[str, Any]], **kwargs) -> None:
        logger.info(
            f"Upserting {len(elements_dict)} elements to "
            f"{self.connection_config.collection_name}",
        )

        qdrant_batch_size = self.upload_config.batch_size

        logger.info(f"using {self.upload_config.num_processes} processes to upload")
        if self.upload_config.num_processes == 1:
            for chunk in batch_generator(elements_dict, qdrant_batch_size):
                self.upsert_batch(chunk)

        else:
            with mp.Pool(
                processes=self.upload_config.num_processes,
            ) as pool:
                pool.map(self.upsert_batch, list(batch_generator(elements_dict, qdrant_batch_size)))

    def run(
        self,
        path: Path,
        file_data: FileData,
        **kwargs: Any,
    ) -> Path:
        docs_list: Dict[Dict[str, Any]] = []

        with path.open("r") as json_file:
            docs_list = json.load(json_file)
        self.write_dict(docs_list=docs_list)


qdrant_destination_entry = DestinationRegistryEntry(
    connection_config=QdrantConnectionConfig,
    uploader=QdrantUploader,
    uploader_config=QdrantUploaderConfig,
    upload_stager=QdrantUploadStager,
    upload_stager_config=QdrantUploadStagerConfig,
)
