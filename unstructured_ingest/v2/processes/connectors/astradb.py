import csv
import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import BaseModel, Field, Secret

from unstructured_ingest import __name__ as integration_name
from unstructured_ingest.__version__ import __version__ as integration_version
from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.data_prep import batch_generator, get_data
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.string_and_date_utils import truncate_string_bytes
from unstructured_ingest.v2.constants import RECORD_ID_LABEL
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    BatchFileData,
    BatchItem,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
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
from unstructured_ingest.v2.processes.connectors.utils import format_and_truncate_orig_elements

if TYPE_CHECKING:
    from astrapy import AsyncCollection as AstraDBAsyncCollection
    from astrapy import Collection as AstraDBCollection
    from astrapy import DataAPIClient as AstraDBClient
    from astrapy import Database as AstraDB


CONNECTOR_TYPE = "astradb"

MAX_CONTENT_PARAM_BYTE_SIZE = 8000


class AstraDBAdditionalMetadata(BaseModel):
    collection_name: str
    keyspace: Optional[str] = None


class AstraDBBatchFileData(BatchFileData):
    additional_metadata: AstraDBAdditionalMetadata


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


def get_astra_db(
    connection_config: AstraDBConnectionConfig,
    keyspace: str,
) -> "AstraDB":
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
    return astra_db


def get_astra_collection(
    connection_config: AstraDBConnectionConfig,
    collection_name: str,
    keyspace: str,
) -> "AstraDBCollection":

    astra_db = get_astra_db(connection_config=connection_config, keyspace=keyspace)

    # astradb will return a collection object in all cases (even if it doesn't exist)
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


class AstraDBIndexerConfig(IndexerConfig):
    collection_name: str = Field(
        description="The name of the Astra DB collection. "
        "Note that the collection name must only include letters, "
        "numbers, and underscores."
    )
    keyspace: Optional[str] = Field(default=None, description="The Astra DB connection keyspace.")
    batch_size: int = Field(default=20, description="Number of records per batch")


@dataclass
class AstraDBIndexer(Indexer):
    connection_config: AstraDBConnectionConfig
    index_config: AstraDBIndexerConfig

    def get_collection(self) -> "AstraDBCollection":
        return get_astra_collection(
            connection_config=self.connection_config,
            collection_name=self.index_config.collection_name,
            keyspace=self.index_config.keyspace,
        )

    def precheck(self) -> None:
        try:
            self.get_collection().options()
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def _get_doc_ids(self) -> set[str]:
        """Fetches all document ids in an index"""
        # Get the collection
        collection = self.get_collection()

        # Perform the find operation to get all items
        astra_db_docs_cursor = collection.find({}, projection={"_id": True})

        # Iterate over the cursor
        astra_db_docs = []
        for result in astra_db_docs_cursor:
            astra_db_docs.append(result)

        # Create file data for each astra record
        ids = sorted([astra_record["_id"] for astra_record in astra_db_docs])

        return set(ids)

    def run(self, **kwargs: Any) -> Generator[AstraDBBatchFileData, None, None]:
        all_ids = self._get_doc_ids()
        ids = list(all_ids)
        id_batches = batch_generator(ids, self.index_config.batch_size)

        for batch in id_batches:
            fd = AstraDBBatchFileData(
                connector_type=CONNECTOR_TYPE,
                metadata=FileDataSourceMetadata(
                    date_processed=str(time()),
                ),
                additional_metadata=AstraDBAdditionalMetadata(
                    collection_name=self.index_config.collection_name,
                    keyspace=self.index_config.keyspace,
                ),
                batch_items=[BatchItem(identifier=b) for b in batch],
            )
            yield fd


class AstraDBDownloaderConfig(DownloaderConfig):
    fields: list[str] = field(default_factory=list)


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

    def generate_download_response(
        self, result: dict, file_data: AstraDBBatchFileData
    ) -> DownloadResponse:
        record_id = result["_id"]
        filename_id = self.get_identifier(record_id=record_id)
        filename = f"{filename_id}.csv"  # csv to preserve column info
        download_path = self.download_dir / Path(filename)
        logger.debug(f"Downloading results from record {record_id} as csv to {download_path}")
        download_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.write_astra_result_to_csv(astra_result=result, download_path=str(download_path))
        except Exception as e:
            logger.error(
                f"failed to download from record {record_id} to {download_path}: {e}",
                exc_info=True,
            )
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")

        # modify input file_data for download_response
        file_data.source_identifiers = SourceIdentifiers(filename=filename, fullpath=filename)
        cast_file_data = FileData.cast(file_data=file_data)
        cast_file_data.identifier = filename
        cast_file_data.metadata.date_processed = str(time())
        cast_file_data.metadata.record_locator = {"document_id": record_id}
        return super().generate_download_response(
            file_data=cast_file_data, download_path=download_path
        )

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        raise NotImplementedError("Use astradb run_async instead")

    async def run_async(self, file_data: FileData, **kwargs: Any) -> download_responses:
        # Get metadata from file_data
        astra_file_data = AstraDBBatchFileData.cast(file_data=file_data)
        ids: list[str] = [item.identifier for item in astra_file_data.batch_items]
        collection_name: str = astra_file_data.additional_metadata.collection_name
        keyspace: str = astra_file_data.additional_metadata.keyspace

        # Retrieve results from async collection
        download_responses = []
        async_astra_collection = await get_async_astra_collection(
            connection_config=self.connection_config,
            collection_name=collection_name,
            keyspace=keyspace,
        )
        async for result in async_astra_collection.find({"_id": {"$in": ids}}):
            download_responses.append(
                self.generate_download_response(result=result, file_data=astra_file_data)
            )
        return download_responses


class AstraDBUploadStagerConfig(UploadStagerConfig):
    flatten_metadata: Optional[bool] = Field(
        default=False, description="Move metadata to top level of the record."
    )


@dataclass
class AstraDBUploadStager(UploadStager):
    upload_stager_config: AstraDBUploadStagerConfig = field(
        default_factory=lambda: AstraDBUploadStagerConfig()
    )

    def truncate_dict_elements(self, element_dict: dict) -> None:
        text = element_dict.pop("text", None)
        if text is not None:
            element_dict["text"] = truncate_string_bytes(text, MAX_CONTENT_PARAM_BYTE_SIZE)
        metadata = element_dict.get("metadata")
        if metadata is not None and isinstance(metadata, dict):
            text_as_html = element_dict["metadata"].pop("text_as_html", None)
            if text_as_html is not None:
                element_dict["metadata"]["text_as_html"] = truncate_string_bytes(
                    text_as_html, MAX_CONTENT_PARAM_BYTE_SIZE
                )
            metadata["original_elements"] = format_and_truncate_orig_elements(element_dict)

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        self.truncate_dict_elements(element_dict)
        if self.upload_stager_config.flatten_metadata:
            # move metadata to top level so it isn't nested in metadata column
            metadata = element_dict.pop("metadata", None)
            if metadata:
                element_dict.update(metadata)

        return {
            "$vector": element_dict.pop("embeddings", None),
            "content": element_dict.pop("text", None),
            RECORD_ID_LABEL: file_data.identifier,
            "metadata": element_dict,
        }


class AstraDBUploaderConfig(UploaderConfig):
    collection_name: Optional[str] = Field(
        description="The name of the Astra DB collection. "
        "Note that the collection name must only include letters, "
        "numbers, and underscores.",
        default=None,
    )
    keyspace: Optional[str] = Field(default=None, description="The Astra DB connection keyspace.")
    requested_indexing_policy: Optional[dict[str, Any]] = Field(
        default=None,
        description="The indexing policy to use for the collection.",
        examples=['{"deny": ["metadata"]}'],
    )
    batch_size: int = Field(default=20, description="Number of records per batch")
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="searchable key to find entries for the same record on previous runs",
    )


@dataclass
class AstraDBUploader(Uploader):
    connection_config: AstraDBConnectionConfig
    upload_config: AstraDBUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def init(self, **kwargs: Any) -> None:
        self.create_destination(**kwargs)

    def precheck(self) -> None:
        try:
            if self.upload_config.collection_name:
                self.get_collection(collection_name=self.upload_config.collection_name).options()
            else:
                # check for db connection only if collection name is not provided
                get_astra_db(
                    connection_config=self.connection_config,
                    keyspace=self.upload_config.keyspace,
                )
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["astrapy"], extras="astradb")
    def get_collection(self, collection_name: Optional[str] = None) -> "AstraDBCollection":
        return get_astra_collection(
            connection_config=self.connection_config,
            collection_name=collection_name or self.upload_config.collection_name,
            keyspace=self.upload_config.keyspace,
        )

    def _collection_exists(self, collection_name: str):
        from astrapy.exceptions import CollectionNotFoundException

        collection = get_astra_collection(
            connection_config=self.connection_config,
            collection_name=collection_name,
            keyspace=self.upload_config.keyspace,
        )

        try:
            collection.options()
            return True
        except CollectionNotFoundException:
            return False
        except Exception as e:
            logger.error(f"failed to check if astra collection exists : {e}")
            raise DestinationConnectionError(f"failed to check if astra collection exists : {e}")

    def format_destination_name(self, destination_name: str) -> str:
        # AstraDB collection naming requirements:
        # must be below 50 characters
        # must be lowercase alphanumeric and underscores only
        formatted = re.sub(r"[^a-z0-9]", "_", destination_name.lower())
        return formatted

    def create_destination(
        self,
        vector_length: int,
        destination_name: str = "unstructuredautocreated",
        similarity_metric: Optional[str] = "cosine",
        **kwargs: Any,
    ) -> bool:
        destination_name = self.format_destination_name(destination_name)
        collection_name = self.upload_config.collection_name or destination_name
        self.upload_config.collection_name = collection_name

        if not self._collection_exists(collection_name):
            astra_db = get_astra_db(
                connection_config=self.connection_config, keyspace=self.upload_config.keyspace
            )
            logger.info(
                f"creating default astra collection '{collection_name}' with dimension "
                f"{vector_length} and metric {similarity_metric}"
            )
            astra_db.create_collection(
                collection_name,
                dimension=vector_length,
                metric=similarity_metric,
            )
            return True
        logger.debug(f"collection with name '{collection_name}' already exists, skipping creation")
        return False

    def delete_by_record_id(self, collection: "AstraDBCollection", file_data: FileData):
        logger.debug(
            f"deleting records from collection {collection.name} "
            f"with {self.upload_config.record_id_key} "
            f"set to {file_data.identifier}"
        )
        delete_filter = {self.upload_config.record_id_key: {"$eq": file_data.identifier}}
        delete_resp = collection.delete_many(filter=delete_filter)
        logger.debug(
            f"deleted {delete_resp.deleted_count} records from collection {collection.name}"
        )

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        logger.info(
            f"writing {len(data)} objects to destination "
            f"collection {self.upload_config.collection_name}"
        )

        astra_db_batch_size = self.upload_config.batch_size
        collection = self.get_collection()

        self.delete_by_record_id(collection=collection, file_data=file_data)

        for chunk in batch_generator(data, astra_db_batch_size):
            collection.insert_many(chunk)

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        data = get_data(path=path)
        self.run_data(data=data, file_data=file_data, **kwargs)


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
