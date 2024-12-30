from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Annotated, Any, Optional

from dateutil import parser
from pydantic import Field, Secret
from pydantic.functional_validators import BeforeValidator

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
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.v2.utils import get_enhanced_element_id

from .utils import conform_string_to_dict

if TYPE_CHECKING:
    from chromadb import Client

CONNECTOR_TYPE = "chroma"


class ChromaAccessConfig(AccessConfig):
    settings: Optional[Annotated[dict, BeforeValidator(conform_string_to_dict)]] = Field(
        default=None, description="A dictionary of settings to communicate with the chroma server."
    )
    headers: Optional[Annotated[dict, BeforeValidator(conform_string_to_dict)]] = Field(
        default=None, description="A dictionary of headers to send to the Chroma server."
    )


class ChromaConnectionConfig(ConnectionConfig):
    access_config: Secret[ChromaAccessConfig] = Field(
        default=ChromaAccessConfig(), validate_default=True
    )
    path: Optional[str] = Field(
        default=None, description="Location where Chroma is persisted, if not connecting via http."
    )
    tenant: Optional[str] = Field(
        default="default_tenant", description="The tenant to use for this client."
    )
    database: Optional[str] = Field(
        default="default_database", description="The database to use for this client."
    )
    host: Optional[str] = Field(default=None, description="The hostname of the Chroma server.")
    port: Optional[int] = Field(default=None, description="The port of the Chroma server.")
    ssl: bool = Field(
        default=False, description="Whether to use SSL to connect to the Chroma server."
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @requires_dependencies(["chromadb"], extras="chroma")
    def get_client(self) -> "Client":
        import chromadb

        access_config = self.access_config.get_secret_value()
        if path := self.path:
            return chromadb.PersistentClient(
                path=path,
                settings=access_config.settings,
                tenant=self.tenant,
                database=self.database,
            )

        elif (host := self.host) and (port := self.port):
            return chromadb.HttpClient(
                host=host,
                port=str(port),
                ssl=self.ssl,
                headers=access_config.headers,
                settings=access_config.settings,
                tenant=self.tenant,
                database=self.database,
            )
        else:
            raise ValueError("Chroma connector requires either path or host and port to be set.")


class ChromaUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class ChromaUploadStager(UploadStager):
    upload_stager_config: ChromaUploadStagerConfig = field(
        default_factory=lambda: ChromaUploadStagerConfig()
    )

    @staticmethod
    def parse_date_string(date_string: str) -> date:
        try:
            timestamp = float(date_string)
            return datetime.fromtimestamp(timestamp)
        except Exception as e:
            logger.debug(f"date {date_string} string not a timestamp: {e}")
        return parser.parse(date_string)

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        """
        Prepares dictionary in the format that Chroma requires
        """
        data = element_dict.copy()
        return {
            "id": get_enhanced_element_id(element_dict=data, file_data=file_data),
            "embedding": data.pop("embeddings", None),
            "document": data.pop("text", None),
            "metadata": flatten_dict(data, separator="-", flatten_lists=True, remove_none=True),
        }


class ChromaUploaderConfig(UploaderConfig):
    collection_name: str = Field(description="The name of the Chroma collection to write into.")
    batch_size: int = Field(default=100, description="Number of records per batch")


@dataclass
class ChromaUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: ChromaUploaderConfig
    connection_config: ChromaConnectionConfig

    def precheck(self) -> None:
        try:
            self.connection_config.get_client()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @DestinationConnectionError.wrap
    def upsert_batch(self, collection, batch):
        try:
            # Chroma wants lists even if there is only one element
            # Upserting to prevent duplicates
            collection.upsert(
                ids=batch["ids"],
                documents=batch["documents"],
                embeddings=batch["embeddings"],
                metadatas=batch["metadatas"],
            )
        except Exception as e:
            raise ValueError(f"chroma error: {e}") from e

    @staticmethod
    def prepare_chroma_list(chunk: tuple[dict[str, Any]]) -> dict[str, list[Any]]:
        """Helper function to break a tuple of dicts into list of parallel lists for ChromaDb.
        ({'id':1}, {'id':2}, {'id':3}) -> {'ids':[1,2,3]}"""
        chroma_dict = {}
        chroma_dict["ids"] = [x.get("id") for x in chunk]
        chroma_dict["documents"] = [x.get("document") for x in chunk]
        chroma_dict["embeddings"] = [x.get("embedding") for x in chunk]
        chroma_dict["metadatas"] = [x.get("metadata") for x in chunk]
        # Make sure all lists are of the same length
        assert (
            len(chroma_dict["ids"])
            == len(chroma_dict["documents"])
            == len(chroma_dict["embeddings"])
            == len(chroma_dict["metadatas"])
        )
        return chroma_dict

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        logger.info(
            f"writing {len(data)} objects to destination "
            f"collection {self.upload_config.collection_name} "
            f"at {self.connection_config.host}",
        )
        client = self.connection_config.get_client()

        collection = client.get_or_create_collection(name=self.upload_config.collection_name)
        for chunk in batch_generator(data, self.upload_config.batch_size):
            self.upsert_batch(collection, self.prepare_chroma_list(chunk))


chroma_destination_entry = DestinationRegistryEntry(
    connection_config=ChromaConnectionConfig,
    uploader=ChromaUploader,
    uploader_config=ChromaUploaderConfig,
    upload_stager=ChromaUploadStager,
    upload_stager_config=ChromaUploadStagerConfig,
)
