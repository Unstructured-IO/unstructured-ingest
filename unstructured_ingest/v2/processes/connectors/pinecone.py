import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
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
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
)

if TYPE_CHECKING:
    from pinecone import Index as PineconeIndex


CONNECTOR_TYPE = "pinecone"


class PineconeAccessConfig(AccessConfig):
    pinecone_api_key: Optional[str] = Field(
        default=None, description="API key for Pinecone.", alias="api_key"
    )


SecretPineconeAccessConfig = Secret[PineconeAccessConfig]


class PineconeConnectionConfig(ConnectionConfig):
    index_name: str = Field(description="Name of the index to connect to.")
    access_config: SecretPineconeAccessConfig = Field(
        default_factory=lambda: SecretPineconeAccessConfig(secret_value=PineconeAccessConfig())
    )

    @requires_dependencies(["pinecone"], extras="pinecone")
    def get_index(self) -> "PineconeIndex":
        from pinecone import Pinecone

        from unstructured_ingest import __version__ as unstructured_version

        pc = Pinecone(
            api_key=self.access_config.get_secret_value().pinecone_api_key,
            source_tag=f"unstructured_ingest=={unstructured_version}",
        )

        index = pc.Index(self.index_name)
        logger.debug(f"Connected to index: {pc.describe_index(self.index_name)}")
        return index


class PineconeUploadStagerConfig(UploadStagerConfig):
    pass


class PineconeUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=100, description="Number of records per batch")


@dataclass
class PineconeUploadStager(UploadStager):
    upload_stager_config: PineconeUploadStagerConfig = field(
        default_factory=lambda: PineconeUploadStagerConfig()
    )

    @staticmethod
    def conform_dict(element_dict: dict) -> dict:
        # While flatten_dict enables indexing on various fields,
        # element_serialized enables easily reloading the element object to memory.
        # element_serialized is formed without text/embeddings to avoid data bloating.
        return {
            "id": str(uuid.uuid4()),
            "values": element_dict.pop("embeddings", None),
            "metadata": {
                "text": element_dict.pop("text", None),
                "element_serialized": json.dumps(element_dict),
                **flatten_dict(
                    element_dict,
                    separator="-",
                    flatten_lists=True,
                    remove_none=True,
                ),
            },
        }

    def run(
        self,
        elements_filepath: Path,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)

        conformed_elements = [
            self.conform_dict(element_dict=element) for element in elements_contents
        ]

        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as output_file:
            json.dump(conformed_elements, output_file)
        return output_path


@dataclass
class PineconeUploader(Uploader):
    upload_config: PineconeUploaderConfig
    connection_config: PineconeConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self):
        try:
            self.connection_config.get_index()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["pinecone"], extras="pinecone")
    def upsert_batch(self, batch):
        from pinecone.exceptions import PineconeApiException

        try:
            index = self.connection_config.get_index()
            response = index.upsert(batch)
        except PineconeApiException as api_error:
            raise DestinationConnectionError(f"http error: {api_error}") from api_error
        logger.debug(f"results: {response}")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with path.open("r") as file:
            elements_dict = json.load(file)
        logger.info(
            f"writing document batches to destination"
            f" index named {self.connection_config.index_name}"
            f" with batch size {self.upload_config.batch_size}"
        )

        pinecone_batch_size = self.upload_config.batch_size
        for pinecone_batch in batch_generator(elements_dict, pinecone_batch_size):
            self.upsert_batch(batch=pinecone_batch)


pinecone_destination_entry = DestinationRegistryEntry(
    connection_config=PineconeConnectionConfig,
    uploader=PineconeUploader,
    uploader_config=PineconeUploaderConfig,
    upload_stager=PineconeUploadStager,
    upload_stager_config=PineconeUploadStagerConfig,
)
