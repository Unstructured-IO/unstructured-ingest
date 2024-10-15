import json
import multiprocessing as mp
from typing import TYPE_CHECKING,Optional,List,Dict,Any
import uuid
from dataclasses import dataclass, field

from pathlib import Path

from datetime import date, datetime

from dateutil import parser
from pydantic import Field

from unstructured_ingest.enhanced_dataclass import enhanced_field
from unstructured_ingest.error import DestinationConnectionError, WriteError
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
from unstructured_ingest.utils.data_prep import batch_generator, flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

if TYPE_CHECKING:
    from qdrant_client import QdrantClient


CONNECTOR_TYPE = "qdrant"


class QdrantAccessConfig(AccessConfig):
    api_key: Optional[str] = enhanced_field(sensitive=True)



class QdrantConnectionConfig(ConnectionConfig):
    collection_name: str
    location: Optional[str] = None
    url: Optional[str] = None
    port: Optional[int] = 6333
    grpc_port: Optional[int] = 6334
    prefer_grpc: Optional[bool] = False
    https: Optional[bool] = None
    prefix: Optional[str] = None
    timeout: Optional[float] = None
    host: Optional[str] = None
    path: Optional[str] = None
    force_disable_check_same_thread: Optional[bool] = False
    access_config: Optional[QdrantAccessConfig] = None

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
    pool_threads: Optional[int] = Field(
        default=1, description="Optional limit on number of threads to use for upload"
    )





















#######################################
#######################################
#######################################
#######################################
#######################################
#######################################
#######################################
#######################################
#######################################
#######################################
#######################################
@dataclass
class QdrantDestinationConnector(IngestDocSessionHandleMixin, BaseDestinationConnector):
    write_config: QdrantWriteConfig
    connector_config: SimpleQdrantConfig
    _client: Optional["QdrantClient"] = None

    @property
    def qdrant_client(self):
        if self._client is None:
            self._client = self.create_client()
        return self._client

    def initialize(self):
        ...  # fmt: skip

    @requires_dependencies(["qdrant_client"], extras="qdrant")
    def create_client(self) -> "QdrantClient":
        from qdrant_client import QdrantClient

        client = QdrantClient(
            location=self.connector_config.location,
            url=self.connector_config.url,
            port=self.connector_config.port,
            grpc_port=self.connector_config.grpc_port,
            prefer_grpc=self.connector_config.prefer_grpc,
            https=self.connector_config.https,
            api_key=(
                self.connector_config.access_config.api_key
                if self.connector_config.access_config
                else None
            ),
            prefix=self.connector_config.prefix,
            timeout=self.connector_config.timeout,
            host=self.connector_config.host,
            path=self.connector_config.path,
            force_disable_check_same_thread=self.connector_config.force_disable_check_same_thread,
        )

        return client

    @DestinationConnectionError.wrap
    def check_connection(self):
        self.qdrant_client.get_collections()

    @DestinationConnectionError.wrap
    @requires_dependencies(["qdrant_client"], extras="qdrant")
    def upsert_batch(self, batch: List[Dict[str, Any]]):
        from qdrant_client import models

        client = self.qdrant_client
        try:
            points: list[models.PointStruct] = [models.PointStruct(**item) for item in batch]
            response = client.upsert(
                self.connector_config.collection_name, points=points, wait=True
            )
        except Exception as api_error:
            raise WriteError(f"Qdrant error: {api_error}") from api_error
        logger.debug(f"results: {response}")

    def write_dict(self, *args, elements_dict: List[Dict[str, Any]], **kwargs) -> None:
        logger.info(
            f"Upserting {len(elements_dict)} elements to "
            f"{self.connector_config.collection_name}",
        )

        qdrant_batch_size = self.write_config.batch_size

        logger.info(f"using {self.write_config.num_processes} processes to upload")
        if self.write_config.num_processes == 1:
            for chunk in batch_generator(elements_dict, qdrant_batch_size):
                self.upsert_batch(chunk)

        else:
            with mp.Pool(
                processes=self.write_config.num_processes,
            ) as pool:
                pool.map(self.upsert_batch, list(batch_generator(elements_dict, qdrant_batch_size)))


