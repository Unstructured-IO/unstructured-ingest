import json
import os
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pytest
from pydantic import BaseModel, SecretStr
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
from pymongo.operations import SearchIndexModel

from test.integration.connectors.utils.constants import DESTINATION_TAG, NOSQL_TAG, SOURCE_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.mongodb import (
    CONNECTOR_TYPE,
    MongoDBAccessConfig,
    MongoDBConnectionConfig,
    MongoDBDownloader,
    MongoDBDownloaderConfig,
    MongoDBIndexer,
    MongoDBIndexerConfig,
    MongoDBUploader,
    MongoDBUploaderConfig,
)

SOURCE_COLLECTION = "sample-mongodb-data"


class EnvData(BaseModel):
    uri: SecretStr
    database: str


def get_env_data() -> EnvData:
    uri = os.getenv("MONGODB_URI")
    assert uri
    database = os.getenv("MONGODB_DATABASE")
    assert database
    return EnvData(uri=uri, database=database)


@contextmanager
def get_client() -> Generator[MongoClient, None, None]:
    uri = get_env_data().uri.get_secret_value()
    with MongoClient(uri) as client:
        assert client.admin.command("ping")
        yield client


def wait_for_collection(
    database: Database, collection_name: str, retries: int = 10, interval: int = 1
):
    collections = database.list_collection_names()
    attempts = 0
    while collection_name not in collections and attempts < retries:
        attempts += 1
        print(
            "Waiting for collection {} to be recognized: {}".format(
                collection_name, ", ".join(collections)
            )
        )
        time.sleep(interval)
        collections = database.list_collection_names()
    if collection_name not in collection_name:
        raise TimeoutError(f"Collection {collection_name} was not recognized")


def get_search_index_status(collection: Collection, index_name: str) -> str:
    search_indexes = collection.list_search_indexes(name=index_name)
    search_index = list(search_indexes)[0]
    return search_index["status"]


def wait_for_search_index(
    collection: Collection, index_name: str, retries: int = 60, interval: int = 1
):
    current_status = get_search_index_status(collection, index_name)
    attempts = 0
    while current_status != "READY" and attempts < retries:
        attempts += 1
        print(f"attempt {attempts}: waiting for search index to be READY: {current_status}")
        time.sleep(interval)
        current_status = get_search_index_status(collection, index_name)

    if current_status != "READY":
        raise TimeoutError("search index never detected as READY")


@pytest.fixture
def destination_collection() -> Collection:
    env_data = get_env_data()
    collection_name = f"utic-test-output-{uuid.uuid4()}"
    with get_client() as client:
        database = client[env_data.database]
        print(f"creating collection in database {database}: {collection_name}")
        collection = database.create_collection(name=collection_name)
        search_index_name = "embeddings"
        collection.create_search_index(
            model=SearchIndexModel(
                name=search_index_name,
                definition={
                    "mappings": {
                        "dynamic": True,
                        "fields": {
                            "embeddings": [
                                {"type": "knnVector", "dimensions": 384, "similarity": "euclidean"}
                            ]
                        },
                    }
                },
            )
        )
        collection.create_index("record_id")
        wait_for_collection(database=database, collection_name=collection_name)
        wait_for_search_index(collection=collection, index_name=search_index_name)
        try:
            yield collection
        finally:
            print(f"deleting collection: {collection_name}")
            collection.drop()


def validate_collection_count(
    collection: Collection, expected_records: int, retries: int = 10, interval: int = 1
) -> None:
    count = collection.count_documents(filter={})
    attempt = 0
    while count != expected_records and attempt < retries:
        attempt += 1
        print(f"attempt {attempt} to get count of collection {count} to match {expected_records}")
        time.sleep(interval)
        count = collection.count_documents(filter={})
    assert (
        count == expected_records
    ), f"expected count ({expected_records}) does not match how many records were found: {count}"


def validate_collection_vector(
    collection: Collection, embedding: list[float], text: str, retries: int = 30, interval: int = 1
) -> None:
    pipeline = [
        {
            "$vectorSearch": {
                "index": "embeddings",
                "path": "embeddings",
                "queryVector": embedding,
                "numCandidates": 150,
                "limit": 10,
            },
        },
        {"$project": {"_id": 0, "text": 1, "score": {"$meta": "vectorSearchScore"}}},
    ]
    attempts = 0
    results = list(collection.aggregate(pipeline=pipeline))
    while not results and attempts < retries:
        attempts += 1
        print(f"attempt {attempts}, waiting for valid results: {results}")
        time.sleep(interval)
        results = list(collection.aggregate(pipeline=pipeline))
    if not results:
        raise TimeoutError("Timed out waiting for valid results")
    print(f"found results on attempt {attempts}")
    top_result = results[0]
    assert top_result["score"] == 1.0, "score detected should be 1: {}".format(top_result["score"])
    assert top_result["text"] == text, "text detected should be {}, found: {}".format(
        text, top_result["text"]
    )
    for r in results[1:]:
        assert r["score"] < 1.0, "score detected should be less than 1: {}".format(r["score"])


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG)
@requires_env("MONGODB_URI", "MONGODB_DATABASE")
async def test_mongodb_source(temp_dir: Path):
    env_data = get_env_data()
    indexer_config = MongoDBIndexerConfig(database=env_data.database, collection=SOURCE_COLLECTION)
    download_config = MongoDBDownloaderConfig(download_dir=temp_dir)
    connection_config = MongoDBConnectionConfig(
        access_config=MongoDBAccessConfig(uri=env_data.uri.get_secret_value()),
    )
    indexer = MongoDBIndexer(connection_config=connection_config, index_config=indexer_config)
    downloader = MongoDBDownloader(
        connection_config=connection_config, download_config=download_config
    )
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id=CONNECTOR_TYPE,
            expected_num_files=4,
            validate_downloaded_files=True,
            expected_number_indexed_file_data=1,
        ),
    )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG)
def test_mongodb_indexer_precheck_fail_no_host():
    indexer_config = MongoDBIndexerConfig(
        database="non-existent-database", collection="non-existent-database"
    )
    connection_config = MongoDBConnectionConfig(
        access_config=MongoDBAccessConfig(uri="mongodb+srv://ingest-test.hgaig.mongodb"),
    )
    indexer = MongoDBIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG)
@requires_env("MONGODB_URI", "MONGODB_DATABASE")
def test_mongodb_indexer_precheck_fail_no_database():
    env_data = get_env_data()
    indexer_config = MongoDBIndexerConfig(
        database="non-existent-database", collection=SOURCE_COLLECTION
    )
    connection_config = MongoDBConnectionConfig(
        access_config=MongoDBAccessConfig(uri=env_data.uri.get_secret_value()),
    )
    indexer = MongoDBIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG)
@requires_env("MONGODB_URI", "MONGODB_DATABASE")
def test_mongodb_indexer_precheck_fail_no_collection():
    env_data = get_env_data()
    indexer_config = MongoDBIndexerConfig(
        database=env_data.database, collection="non-existent-collection"
    )
    connection_config = MongoDBConnectionConfig(
        access_config=MongoDBAccessConfig(uri=env_data.uri.get_secret_value()),
    )
    indexer = MongoDBIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
@requires_env("MONGODB_URI", "MONGODB_DATABASE")
async def test_mongodb_destination(
    upload_file: Path,
    destination_collection: Collection,
    tmp_path: Path,
):
    env_data = get_env_data()
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mongodb_mock_id",
    )
    connection_config = MongoDBConnectionConfig(
        access_config=MongoDBAccessConfig(uri=env_data.uri.get_secret_value()),
    )

    upload_config = MongoDBUploaderConfig(
        database=env_data.database,
        collection=destination_collection.name,
    )
    uploader = MongoDBUploader(connection_config=connection_config, upload_config=upload_config)
    uploader.precheck()
    uploader.run(path=upload_file, file_data=file_data)

    with upload_file.open() as f:
        staged_elements = json.load(f)
    expected_records = len(staged_elements)
    validate_collection_count(collection=destination_collection, expected_records=expected_records)
    first_element = staged_elements[0]
    validate_collection_vector(
        collection=destination_collection,
        embedding=first_element["embeddings"],
        text=first_element["text"],
    )

    uploader.run(path=upload_file, file_data=file_data)
    validate_collection_count(collection=destination_collection, expected_records=expected_records)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_mongodb_uploader_precheck_fail_no_host():
    upload_config = MongoDBUploaderConfig(
        database="database",
        collection="collection",
    )
    connection_config = MongoDBConnectionConfig(
        access_config=MongoDBAccessConfig(uri="mongodb+srv://ingest-test.hgaig.mongodb"),
    )
    uploader = MongoDBUploader(connection_config=connection_config, upload_config=upload_config)
    with pytest.raises(DestinationConnectionError):
        uploader.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
@requires_env("MONGODB_URI", "MONGODB_DATABASE")
def test_mongodb_uploader_precheck_fail_no_database():
    env_data = get_env_data()
    upload_config = MongoDBUploaderConfig(
        database="database",
        collection="collection",
    )
    connection_config = MongoDBConnectionConfig(
        access_config=MongoDBAccessConfig(uri=env_data.uri.get_secret_value()),
    )
    uploader = MongoDBUploader(connection_config=connection_config, upload_config=upload_config)
    with pytest.raises(DestinationConnectionError):
        uploader.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
@requires_env("MONGODB_URI", "MONGODB_DATABASE")
def test_mongodb_uploader_precheck_fail_no_collection():
    env_data = get_env_data()
    upload_config = MongoDBUploaderConfig(
        database=env_data.database,
        collection="collection",
    )
    connection_config = MongoDBConnectionConfig(
        access_config=MongoDBAccessConfig(uri=env_data.uri.get_secret_value()),
    )
    uploader = MongoDBUploader(connection_config=connection_config, upload_config=upload_config)
    with pytest.raises(DestinationConnectionError):
        uploader.precheck()
