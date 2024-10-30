import copy
import csv
import hashlib
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional

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
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
    download_responses,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)

if TYPE_CHECKING:
    from astrapy import AsyncCollection as AstraDBAsyncCollection
    from astrapy import Collection as AstraDBCollection
    from astrapy import DataAPIClient as AstraDBClient


CONNECTOR_TYPE = "astradb"


class AstraDBAccessConfig(AccessConfig):
    token: str = Field(description="Astra DB Token with access to the database.")
    api_endpoint: str = Field(description="The API endpoint for the Astra DB.")


class AstraDBConnectionConfig(ConnectionConfig):
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    access_config: Secret[AstraDBAccessConfig]

    @requires_dependencies(["astrapy"], extras="astradb")
    def get_client(self) -> "AstraDBClient":
        from astrapy import DataAPIClient as AstraDBClient

        # Create a client object to interact with the Astra DB
        # caller_name/version for Astra DB tracking
        return AstraDBClient(
            caller_name=integration_name,
            caller_version=integration_version,
        )


def get_astra_collection(
    connection_config: AstraDBConnectionConfig,
    collection_name: str,
    keyspace: str,
) -> "AstraDBCollection":
    # Build the Astra DB object.
    access_configs = connection_config.access_config.get_secret_value()

    # Create a client object to interact with the Astra DB
    # caller_name/version for Astra DB tracking
    client = connection_config.get_client()

    # Get the database object
    astra_db = client.get_database(
        api_endpoint=access_configs.api_endpoint,
        token=access_configs.token,
        keyspace=keyspace,
    )

    # Connect to the collection
    astra_db_collection = astra_db.get_collection(name=collection_name)
    return astra_db_collection


async def get_async_astra_collection(
    connection_config: AstraDBConnectionConfig,
    collection_name: str,
    keyspace: str,
) -> "AstraDBAsyncCollection":
    # Build the Astra DB object.
    access_configs = connection_config.access_config.get_secret_value()

    # Create a client object to interact with the Astra DB
    client = connection_config.get_client()

    # Get the async database object
    async_astra_db = client.get_async_database(
        api_endpoint=access_configs.api_endpoint,
        token=access_configs.token,
        keyspace=keyspace,
    )

    # Get async collection from AsyncDatabase
    async_astra_db_collection = await async_astra_db.get_collection(name=collection_name)
    return async_astra_db_collection


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
    batch_size: int = Field(default=20, description="Number of records per batch")


class AstraDBDownloaderConfig(DownloaderConfig):
    fields: list[str] = field(default_factory=list)


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

    def get_collection(self) -> "AstraDBCollection":
        return get_astra_collection(
            connection_config=self.connection_config,
            collection_name=self.index_config.collection_name,
            keyspace=self.index_config.keyspace or self.index_config.namespace,
        )

    def precheck(self) -> None:
        try:
            self.get_collection()
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def _get_doc_ids(self) -> set[str]:
        """Fetches all document ids in an index"""
        # Initialize set of ids
        ids = set()

        # Get the collection
        collection = self.get_collection()

        # Perform the find operation to get all items
        astra_db_docs_cursor = collection.find({}, projection={"_id": True})

        # Iterate over the cursor
        astra_db_docs = []
        for result in astra_db_docs_cursor:
            astra_db_docs.append(result)

        # Create file data for each astra record
        for astra_record in astra_db_docs:
            ids.add(astra_record["_id"])

        return ids

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        all_ids = self._get_doc_ids()
        ids = list(all_ids)
        id_batches = batch_generator(ids, self.index_config.batch_size)

        for batch in id_batches:
            # Make sure the hash is always a positive number to create identified
            identified = str(hash(batch) + sys.maxsize + 1)
            fd = FileData(
                identifier=identified,
                connector_type=CONNECTOR_TYPE,
                doc_type="batch",
                metadata=FileDataSourceMetadata(
                    date_processed=str(time()),
                ),
                additional_metadata={
                    "ids": list(batch),
                    "collection_name": self.index_config.collection_name,
                    "keyspace": self.index_config.keyspace or self.index_config.namespace,
                },
            )
            yield fd


@dataclass
class AstraDBDownloader(Downloader):
    connection_config: AstraDBConnectionConfig
    download_config: AstraDBDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        return True

    def get_identifier(self, record_id: str) -> str:
        f = f"{record_id}"
        if self.download_config.fields:
            f = "{}-{}".format(
                f,
                hashlib.sha256(",".join(self.download_config.fields).encode()).hexdigest()[:8],
            )
        return f

    def write_astra_result_to_csv(self, astra_result: dict, download_path: str) -> None:
        with open(download_path, "w", encoding="utf8") as f:
            writer = csv.writer(f)
            writer.writerow(astra_result.keys())
            writer.writerow(astra_result.values())

    def generate_download_response(self, result: dict, file_data: FileData) -> DownloadResponse:
        record_id = result["_id"]
        filename_id = self.get_identifier(record_id=record_id)
        filename = f"{filename_id}.csv"  # csv to preserve column info
        download_path = self.download_dir / Path(filename)
        logger.debug(f"Downloading results from record {record_id} as csv to {download_path}")
        download_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.write_astra_result_to_csv(astra_result=result, download_path=download_path)
        except Exception as e:
            logger.error(
                f"failed to download from record {record_id} to {download_path}: {e}",
                exc_info=True,
            )
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")

        # modify input file_data for download_response
        copied_file_data = copy.deepcopy(file_data)
        copied_file_data.identifier = filename
        copied_file_data.doc_type = "file"
        copied_file_data.metadata.date_processed = str(time())
        copied_file_data.metadata.record_locator = {"document_id": record_id}
        copied_file_data.additional_metadata.pop("ids", None)
        return super().generate_download_response(
            file_data=copied_file_data, download_path=download_path
        )

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        raise NotImplementedError("Use astradb run_async instead")

    async def run_async(self, file_data: FileData, **kwargs: Any) -> download_responses:
        # Get metadata from file_data
        ids: list[str] = file_data.additional_metadata["ids"]
        collection_name: str = file_data.additional_metadata["collection_name"]
        keyspace: str = file_data.additional_metadata["keyspace"]

        # Retrieve results from async collection
        download_responses = []
        async_astra_collection = await get_async_astra_collection(
            connection_config=self.connection_config,
            collection_name=collection_name,
            keyspace=keyspace,
        )
        async for result in async_astra_collection.find({"_id": {"$in": ids}}):
            download_responses.append(
                self.generate_download_response(result=result, file_data=file_data)
            )
        return download_responses


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
                connection_config=self.connection_config,
                collection_name=self.upload_config.collection_name,
                keyspace=self.upload_config.keyspace or self.upload_config.namespace,
            )
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["astrapy"], extras="astradb")
    def get_collection(self) -> "AstraDBCollection":
        return get_astra_collection(
            connection_config=self.connection_config,
            collection_name=self.upload_config.collection_name,
            keyspace=self.upload_config.keyspace or self.upload_config.namespace,
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
