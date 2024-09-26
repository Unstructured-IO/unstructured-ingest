import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field, Secret

from unstructured_ingest import __name__ as integration_name
from unstructured_ingest.__version__ import __version__ as integration_version
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import batch_generator
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
    from astrapy import Collection as AstraDBCollection


CONNECTOR_TYPE = "astradb"


class AstraDBAccessConfig(AccessConfig):
    token: str = Field(description="Astra DB Token with access to the database.")
    api_endpoint: str = Field(description="The API endpoint for the Astra DB.")


class AstraDBConnectionConfig(ConnectionConfig):
    connection_type: str = Field(default=CONNECTOR_TYPE, init=False)
    access_config: Secret[AstraDBAccessConfig]


class AstraDBUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class AstraDBUploadStager(UploadStager):
    upload_stager_config: AstraDBUploadStagerConfig = field(
        default_factory=lambda: AstraDBUploadStagerConfig()
    )

    def conform_dict(self, element_dict: dict) -> dict:
        return {
            "$vector": element_dict.pop("embeddings", None),
            "content": element_dict.pop("text", None),
            "metadata": element_dict,
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
        conformed_elements = []
        for element in elements_contents:
            conformed_elements.append(self.conform_dict(element_dict=element))
        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        with open(output_path, "w") as output_file:
            json.dump(conformed_elements, output_file)
        return output_path


class AstraDBUploaderConfig(UploaderConfig):
    collection_name: str = Field(
        description="The name of the Astra DB collection. "
        "Note that the collection name must only include letters, "
        "numbers, and underscores."
    )
    embedding_dimension: int = Field(
        default=384, description="The dimensionality of the embeddings"
    )
    keyspace: Optional[str] = Field(default=None, description="The Astra DB connection keyspace.")
    namespace: Optional[str] = Field(
        default=None,
        description="The Astra DB connection namespace.",
        deprecated="Please use 'keyspace' instead.",
    )
    requested_indexing_policy: Optional[dict[str, Any]] = Field(
        default=None,
        description="The indexing policy to use for the collection.",
        examples=['{"deny": ["metadata"]}'],
    )
    batch_size: int = Field(default=20, description="Number of records per batch")


@dataclass
class AstraDBUploader(Uploader):
    connection_config: AstraDBConnectionConfig
    upload_config: AstraDBUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            self.get_collection()
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["astrapy"], extras="astradb")
    def get_collection(self) -> "AstraDBCollection":
        from astrapy import DataAPIClient as AstraDBClient

        # Choose keyspace or deprecated namespace
        keyspace_param = self.upload_config.keyspace or self.upload_config.namespace

        # Get the collection_name
        collection_name = self.upload_config.collection_name

        # Build the Astra DB object.
        access_configs = self.connection_config.access_config.get_secret_value()

        # Create a client object to interact with the Astra DB
        # caller_name/version for Astra DB tracking
        my_client = AstraDBClient(
            caller_name=integration_name,
            caller_version=integration_version,
        )

        # Get the database object
        astra_db = my_client.get_database(
            api_endpoint=access_configs.api_endpoint,
            token=access_configs.token,
            keyspace=keyspace_param,
        )

        # Connect to the newly created collection
        astra_db_collection = astra_db.get_collection(name=collection_name)

        return astra_db_collection

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with path.open("r") as file:
            elements_dict = json.load(file)
        logger.info(
            f"writing {len(elements_dict)} objects to destination "
            f"collection {self.upload_config.collection_name}"
        )

        astra_db_batch_size = self.upload_config.batch_size
        collection = self.get_collection()

        for chunk in batch_generator(elements_dict, astra_db_batch_size):
            collection.insert_many(chunk)


astra_db_destination_entry = DestinationRegistryEntry(
    connection_config=AstraDBConnectionConfig,
    upload_stager_config=AstraDBUploadStagerConfig,
    upload_stager=AstraDBUploadStager,
    uploader_config=AstraDBUploaderConfig,
    uploader=AstraDBUploader,
)
