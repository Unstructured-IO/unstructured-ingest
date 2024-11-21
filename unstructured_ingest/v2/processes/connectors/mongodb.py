import json
import sys
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.__version__ import __version__ as unstructured_version
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
from unstructured_ingest.utils.data_prep import batch_generator, flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.constants import RECORD_ID_LABEL
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
    Uploader,
    UploaderConfig,
    download_responses,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)

if TYPE_CHECKING:
    from pymongo import MongoClient
    from pymongo.collection import Collection

CONNECTOR_TYPE = "mongodb"
SERVER_API_VERSION = "1"


class MongoDBAccessConfig(AccessConfig):
    uri: Optional[str] = Field(default=None, description="URI to user when connecting")


class MongoDBConnectionConfig(ConnectionConfig):
    access_config: Secret[MongoDBAccessConfig] = Field(
        default=MongoDBAccessConfig(), validate_default=True
    )
    host: Optional[str] = Field(
        default=None,
        description="hostname or IP address or Unix domain socket path of a single mongod or "
        "mongos instance to connect to, or a list of hostnames",
    )
    port: int = Field(default=27017)
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @contextmanager
    @requires_dependencies(["pymongo"], extras="mongodb")
    def get_client(self) -> Generator["MongoClient", None, None]:
        from pymongo import MongoClient
        from pymongo.driver_info import DriverInfo
        from pymongo.server_api import ServerApi

        access_config = self.access_config.get_secret_value()
        if uri := access_config.uri:
            client_kwargs = {
                "host": uri,
                "server_api": ServerApi(version=SERVER_API_VERSION),
                "driver": DriverInfo(name="unstructured", version=unstructured_version),
            }
        else:
            client_kwargs = {
                "host": self.host,
                "port": self.port,
                "server_api": ServerApi(version=SERVER_API_VERSION),
            }
        with MongoClient(**client_kwargs) as client:
            yield client


class MongoDBIndexerConfig(IndexerConfig):
    batch_size: int = Field(default=100, description="Number of records per batch")
    database: Optional[str] = Field(default=None, description="database name to connect to")
    collection: Optional[str] = Field(default=None, description="collection name to connect to")


class MongoDBDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class MongoDBIndexer(Indexer):
    connection_config: MongoDBConnectionConfig
    index_config: MongoDBIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        """Validates the connection to the MongoDB server."""
        try:
            with self.connection_config.get_client() as client:
                client.admin.command("ping")
                database_names = client.list_database_names()
                database_name = self.index_config.database
                if database_name not in database_names:
                    raise DestinationConnectionError(
                        "database {} does not exist: {}".format(
                            database_name, ", ".join(database_names)
                        )
                    )
                database = client[database_name]
                collection_names = database.list_collection_names()
                collection_name = self.index_config.collection
                if collection_name not in collection_names:
                    raise SourceConnectionError(
                        "collection {} does not exist: {}".format(
                            collection_name, ", ".join(collection_names)
                        )
                    )
        except Exception as e:
            logger.error(f"Failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        """Generates FileData objects for each document in the MongoDB collection."""
        with self.connection_config.get_client() as client:
            database = client[self.index_config.database]
            collection = database[self.index_config.collection]

            # Get list of document IDs
            ids = collection.distinct("_id")
            batch_size = self.index_config.batch_size if self.index_config else 100

        for id_batch in batch_generator(ids, batch_size=batch_size):
            # Make sure the hash is always a positive number to create identifier
            batch_id = str(hash(frozenset(id_batch)) + sys.maxsize + 1)

            metadata = FileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "database": self.index_config.database,
                    "collection": self.index_config.collection,
                },
            )

            file_data = FileData(
                identifier=batch_id,
                doc_type="batch",
                connector_type=self.connector_type,
                metadata=metadata,
                additional_metadata={
                    "ids": [str(doc_id) for doc_id in id_batch],
                },
            )
            yield file_data


@dataclass
class MongoDBDownloader(Downloader):
    download_config: MongoDBDownloaderConfig
    connection_config: MongoDBConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["pymongo"], extras="mongodb")
    def create_client(self) -> "MongoClient":
        from pymongo import MongoClient
        from pymongo.driver_info import DriverInfo
        from pymongo.server_api import ServerApi

        access_config = self.connection_config.access_config.get_secret_value()

        if access_config.uri:
            return MongoClient(
                access_config.uri,
                server_api=ServerApi(version=SERVER_API_VERSION),
                driver=DriverInfo(name="unstructured", version=unstructured_version),
            )
        else:
            return MongoClient(
                host=self.connection_config.host,
                port=self.connection_config.port,
                server_api=ServerApi(version=SERVER_API_VERSION),
            )

    @SourceConnectionError.wrap
    @requires_dependencies(["bson"], extras="mongodb")
    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        """Fetches the document from MongoDB and writes it to a file."""
        from bson.errors import InvalidId
        from bson.objectid import ObjectId

        client = self.create_client()
        database = client[file_data.metadata.record_locator["database"]]
        collection = database[file_data.metadata.record_locator["collection"]]

        ids = file_data.additional_metadata.get("ids", [])
        if not ids:
            raise ValueError("No document IDs provided in additional_metadata")

        object_ids = []
        for doc_id in ids:
            try:
                object_ids.append(ObjectId(doc_id))
            except InvalidId as e:
                error_message = f"Invalid ObjectId for doc_id '{doc_id}': {str(e)}"
                logger.error(error_message)
                raise ValueError(error_message) from e

        try:
            docs = list(collection.find({"_id": {"$in": object_ids}}))
        except Exception as e:
            logger.error(f"Failed to fetch documents: {e}", exc_info=True)
            raise e

        download_responses = []
        for doc in docs:
            doc_id = doc["_id"]
            doc.pop("_id", None)

            # Extract date_created from the document or ObjectId
            date_created = None
            if "date_created" in doc:
                # If the document has a 'date_created' field, use it
                date_created = doc["date_created"]
                if isinstance(date_created, datetime):
                    date_created = date_created.isoformat()
                else:
                    # Convert to ISO format if it's a string
                    date_created = str(date_created)
            elif isinstance(doc_id, ObjectId):
                # Use the ObjectId's generation time
                date_created = doc_id.generation_time.isoformat()

            flattened_dict = flatten_dict(dictionary=doc)
            concatenated_values = "\n".join(str(value) for value in flattened_dict.values())

            # Create a FileData object for each document with source_identifiers
            individual_file_data = replace(file_data)
            individual_file_data.identifier = str(doc_id)
            individual_file_data.source_identifiers = SourceIdentifiers(
                filename=str(doc_id),
                fullpath=str(doc_id),
                rel_path=str(doc_id),
            )

            # Determine the download path
            download_path = self.get_download_path(individual_file_data)
            if download_path is None:
                raise ValueError("Download path could not be determined")

            download_path.parent.mkdir(parents=True, exist_ok=True)
            download_path = download_path.with_suffix(".txt")

            # Write the concatenated values to the file
            with open(download_path, "w", encoding="utf8") as f:
                f.write(concatenated_values)

            individual_file_data.local_download_path = str(download_path)

            # Update metadata
            individual_file_data.metadata.record_locator["document_id"] = str(doc_id)
            individual_file_data.metadata.date_created = date_created

            download_response = self.generate_download_response(
                file_data=individual_file_data, download_path=download_path
            )
            download_responses.append(download_response)

        return download_responses


class MongoDBUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=100, description="Number of records per batch")
    database: Optional[str] = Field(default=None, description="database name to connect to")
    collection: Optional[str] = Field(default=None, description="collection name to connect to")
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="searchable key to find entries for the same record on previous runs",
    )


@dataclass
class MongoDBUploader(Uploader):
    upload_config: MongoDBUploaderConfig
    connection_config: MongoDBConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            with self.connection_config.get_client() as client:
                client.admin.command("ping")
                database_names = client.list_database_names()
                database_name = self.upload_config.database
                if database_name not in database_names:
                    raise DestinationConnectionError(
                        "database {} does not exist: {}".format(
                            database_name, ", ".join(database_names)
                        )
                    )
                database = client[database_name]
                collection_names = database.list_collection_names()
                collection_name = self.upload_config.collection
                if collection_name not in collection_names:
                    raise SourceConnectionError(
                        "collection {} does not exist: {}".format(
                            collection_name, ", ".join(collection_names)
                        )
                    )
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def can_delete(self, collection: "Collection") -> bool:
        indexed_keys = []
        for index in collection.list_indexes():
            key_bson = index["key"]
            indexed_keys.extend(key_bson.keys())
        return self.upload_config.record_id_key in indexed_keys

    def delete_by_record_id(self, collection: "Collection", file_data: FileData) -> None:
        logger.debug(
            f"deleting any content with metadata "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from collection: {collection.name}"
        )
        query = {self.upload_config.record_id_key: file_data.identifier}
        delete_results = collection.delete_many(filter=query)
        logger.info(
            f"deleted {delete_results.deleted_count} records from collection {collection.name}"
        )

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with path.open("r") as file:
            elements_dict = json.load(file)
        logger.info(
            f"writing {len(elements_dict)} objects to destination "
            f"db, {self.upload_config.database}, "
            f"collection {self.upload_config.collection} "
            f"at {self.connection_config.host}",
        )
        # This would typically live in the stager but since no other manipulation
        # is done, setting the record id field in the uploader
        for element in elements_dict:
            element[self.upload_config.record_id_key] = file_data.identifier
        with self.connection_config.get_client() as client:
            db = client[self.upload_config.database]
            collection = db[self.upload_config.collection]
            if self.can_delete(collection=collection):
                self.delete_by_record_id(file_data=file_data, collection=collection)
            else:
                logger.warning("criteria for deleting previous content not met, skipping")
            for chunk in batch_generator(elements_dict, self.upload_config.batch_size):
                collection.insert_many(chunk)


mongodb_destination_entry = DestinationRegistryEntry(
    connection_config=MongoDBConnectionConfig,
    uploader=MongoDBUploader,
    uploader_config=MongoDBUploaderConfig,
)

mongodb_source_entry = SourceRegistryEntry(
    connection_config=MongoDBConnectionConfig,
    indexer_config=MongoDBIndexerConfig,
    indexer=MongoDBIndexer,
    downloader_config=MongoDBDownloaderConfig,
    downloader=MongoDBDownloader,
)
