import json
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional
import sys
from pydantic import Field, Secret

from unstructured_ingest.__version__ import __version__ as unstructured_version
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
from unstructured_ingest.utils.data_prep import batch_generator, flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
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
    from pymongo import MongoClient

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
    database: Optional[str] = Field(default=None, description="database name to connect to")
    collection: Optional[str] = Field(default=None, description="collection name to connect to")
    port: int = Field(default=27017)
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)


class MongoDBUploadStagerConfig(UploadStagerConfig):
    pass


class MongoDBIndexerConfig(IndexerConfig):
    batch_size: int = Field(default=100, description="Number of records per batch")


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
            client = self.create_client()
            client.admin.command("ping")
        except Exception as e:
            logger.error(f"Failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")

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

    @requires_dependencies(["pymongo"], extras="mongodb")
    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        """Generates FileData objects for each document in the MongoDB collection."""
        client = self.create_client()
        database = client[self.connection_config.database]
        collection = database[self.connection_config.collection]

        # Get list of document IDs
        ids = collection.distinct("_id")
        batch_size = self.index_config.batch_size if self.index_config else 100
        id_batches = [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]

        for id_batch in id_batches:
            batch_id = str(hash(frozenset(id_batch)) + sys.maxsize + 1)

            metadata = FileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "database": self.connection_config.database,
                    "collection": self.connection_config.collection,
                },
            )

            file_data = FileData(
                identifier=batch_id,
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
    @requires_dependencies(["pymongo"], extras="mongodb")
    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        """Fetches the document from MongoDB and writes it to a file."""
        from bson.objectid import ObjectId

        client = self.create_client()
        database = client[self.connection_config.database]
        collection = database[self.connection_config.collection]

        ids = file_data.additional_metadata.get("ids", [])
        if not ids:
            raise ValueError("No document IDs provided in additional_metadata")

        object_ids = []
        for doc_id in ids:
            try:
                object_ids.append(ObjectId(doc_id))
            except Exception:
                object_ids.append(doc_id)

        try:
            docs = list(collection.find({"_id": {"$in": object_ids}}))
        except Exception as e:
            logger.error(f"Failed to fetch documents: {e}", exc_info=True)
            raise e

        download_responses = []
        for doc in docs:
            doc_id = doc["_id"]
            doc.pop("_id", None)

            flattened_dict = flatten_dict(dictionary=doc)
            concatenated_values = "\n".join(str(value) for value in flattened_dict.values())

            individual_file_data = FileData(
                identifier=str(doc_id),
                connector_type=self.connector_type,
            )

            download_path = self.get_download_path(individual_file_data)
            if download_path is None:
                raise ValueError("Download path could not be determined")

            download_path.parent.mkdir(parents=True, exist_ok=True)
            download_path = download_path.with_suffix(".txt")

            with open(download_path, "w", encoding="utf8") as f:
                f.write(concatenated_values)

            individual_file_data.local_download_path = str(download_path)

            individual_file_data.metadata = FileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "database": self.connection_config.database,
                    "collection": self.connection_config.collection,
                    "document_id": str(doc_id),
                },
            )

            download_response = self.generate_download_response(
                file_data=individual_file_data, download_path=download_path
            )
            download_responses.append(download_response)

        return download_responses


@dataclass
class MongoDBUploadStager(UploadStager):
    upload_stager_config: MongoDBUploadStagerConfig = field(
        default_factory=lambda: MongoDBUploadStagerConfig()
    )

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
        with open(output_path, "w") as output_file:
            json.dump(elements_contents, output_file)
        return output_path


class MongoDBUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=100, description="Number of records per batch")


@dataclass
class MongoDBUploader(Uploader):
    upload_config: MongoDBUploaderConfig
    connection_config: MongoDBConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            client = self.create_client()
            client.admin.command("ping")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

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

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with path.open("r") as file:
            elements_dict = json.load(file)
        logger.info(
            f"writing {len(elements_dict)} objects to destination "
            f"db, {self.connection_config.database}, "
            f"collection {self.connection_config.collection} "
            f"at {self.connection_config.host}",
        )
        client = self.create_client()
        db = client[self.connection_config.database]
        collection = db[self.connection_config.collection]
        for chunk in batch_generator(elements_dict, self.upload_config.batch_size):
            collection.insert_many(chunk)


mongodb_destination_entry = DestinationRegistryEntry(
    connection_config=MongoDBConnectionConfig,
    uploader=MongoDBUploader,
    uploader_config=MongoDBUploaderConfig,
    upload_stager=MongoDBUploadStager,
    upload_stager_config=MongoDBUploadStagerConfig,
)

mongodb_source_entry = SourceRegistryEntry(
    connection_config=MongoDBConnectionConfig,
    indexer_config=MongoDBIndexerConfig,
    indexer=MongoDBIndexer,
    downloader_config=MongoDBDownloaderConfig,
    downloader=MongoDBDownloader,
)
