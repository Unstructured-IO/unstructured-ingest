import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest import __name__ as integration_name
from unstructured_ingest.__version__ import __version__ as integration_version
from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    Indexer,
    IndexerConfig,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
    download_responses,
)
from unstructured_ingest.v2.interfaces.file_data import SourceIdentifiers
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)

if TYPE_CHECKING:
    from astrapy import Collection as AstraDBCollection


CONNECTOR_TYPE = "astradb"


def get_astra_collection(connection_config, config) -> "AstraDBCollection":
    from astrapy import DataAPIClient as AstraDBClient

    # Choose keyspace or deprecated namespace
    keyspace_param = config.keyspace or config.namespace

    # Get the collection_name
    collection_name = config.collection_name

    # Build the Astra DB object.
    access_configs = connection_config.access_config.get_secret_value()

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


class AstraDBAccessConfig(AccessConfig):
    token: str = Field(description="Astra DB Token with access to the database.")
    api_endpoint: str = Field(description="The API endpoint for the Astra DB.")


class AstraDBConnectionConfig(ConnectionConfig):
    connection_type: str = Field(default=CONNECTOR_TYPE, init=False)
    access_config: Secret[AstraDBAccessConfig]


class AstraDBUploadStagerConfig(UploadStagerConfig):
    pass


class AstraDBIndexerConfig(IndexerConfig):
    collection_name: str = Field(
        description="The name of the Astra DB collection. "
        "Note that the collection name must only include letters, "
        "numbers, and underscores."
    )
    keyspace: Optional[str] = Field(default=None, description="The Astra DB connection keyspace.")
    namespace: Optional[str] = Field(
        default=None,
        description="The Astra DB connection namespace.",
        deprecated="Please use 'keyspace' instead.",
    )


class AstraDBDownloaderConfig(DownloaderConfig):
    collection_name: str = Field(
        description="The name of the Astra DB collection. "
        "Note that the collection name must only include letters, "
        "numbers, and underscores."
    )
    keyspace: Optional[str] = Field(default=None, description="The Astra DB connection keyspace.")
    namespace: Optional[str] = Field(
        default=None,
        description="The Astra DB connection namespace.",
        deprecated="Please use 'keyspace' instead.",
    )


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
class AstraDBIndexer(Indexer):
    connection_config: AstraDBConnectionConfig
    index_config: AstraDBIndexerConfig

    def precheck(self) -> None:
        try:
            get_astra_collection(connection_config=self.connection_config, config=self.index_config)
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["astrapy"], extras="astradb")
    def get_collection(self) -> "AstraDBCollection":
        return get_astra_collection(
            connection_config=self.connection_config,
            config=self.index_config,
        )

    @requires_dependencies(["astrapy"], extras="astradb")
    def astra_record_to_file_data(self, astra_record: Dict[str, Any]) -> FileData:
        return FileData(
            identifier=astra_record["_id"],
            connector_type=CONNECTOR_TYPE,
            source_identifiers=SourceIdentifiers(
                fullpath=astra_record["_id"] + ".txt",
                filename=astra_record["_id"] + ".txt",
                rel_path=astra_record["_id"] + ".txt",
            ),
            additional_metadata=astra_record.get("metadata", {}),
        )

    @requires_dependencies(["astrapy"], extras="astradb")
    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        # Get the collection
        collection = self.get_collection()

        # Perform the find operation to get all items
        astra_db_docs_cursor = collection.find({}, projection={"_id": True, "metadata": True})

        # Iterate over the cursor
        astra_db_docs = []
        for result in astra_db_docs_cursor:
            astra_db_docs.append(result)

        # Create file data for each astra record
        for astra_record in astra_db_docs:
            file_data = self.astra_record_to_file_data(astra_record=astra_record)

            yield file_data


@dataclass
class AstraDBDownloader(Downloader):
    connection_config: AstraDBConnectionConfig
    download_config: AstraDBDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @SourceConnectionNetworkError.wrap
    def _fetch_record(self, file_data: FileData):
        collection = get_astra_collection(
            connection_config=self.connection_config,
            config=self.download_config,
        )

        record = collection.find_one({"_id": file_data.identifier})

        return record

    def get_download_path(self, file_data: FileData) -> Path:
        # Get the relative path from the source identifiers
        rel_path = ""
        if file_data.source_identifiers and file_data.source_identifiers.rel_path:
            rel_path = file_data.source_identifiers.rel_path

        # Remove leading slash if present
        rel_path = rel_path[1:] if rel_path.startswith("/") else rel_path

        return self.download_dir / Path(rel_path)

    @SourceConnectionError.wrap
    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        # Fetch the record from the collection
        record = self._fetch_record(file_data=file_data)

        # Get the download path
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)

        # Write "record" to a json file at download path
        with open(download_path, "w") as file:
            # List comp. to prevent newlines at the end of the file which affects partitioning
            file.write("\n".join([str(x) for x in record.values()]))

        return DownloadResponse(file_data=file_data, path=download_path)


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


@dataclass
class AstraDBUploader(Uploader):
    connection_config: AstraDBConnectionConfig
    upload_config: AstraDBUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            get_astra_collection(
                connection_config=self.connection_config, config=self.upload_config
            )
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["astrapy"], extras="astradb")
    def get_collection(self) -> "AstraDBCollection":
        return get_astra_collection(
            connection_config=self.connection_config,
            config=self.upload_config,
        )

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


astra_db_source_entry = SourceRegistryEntry(
    indexer=AstraDBIndexer,
    indexer_config=AstraDBIndexerConfig,
    downloader=AstraDBDownloader,
    downloader_config=AstraDBDownloaderConfig,
    connection_config=AstraDBConnectionConfig,
)

astra_db_destination_entry = DestinationRegistryEntry(
    connection_config=AstraDBConnectionConfig,
    upload_stager_config=AstraDBUploadStagerConfig,
    upload_stager=AstraDBUploadStager,
    uploader_config=AstraDBUploaderConfig,
    uploader=AstraDBUploader,
)
