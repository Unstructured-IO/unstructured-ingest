import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pytest
from pydantic import BaseModel, SecretStr
from pymongo.mongo_client import MongoClient

from test.integration.connectors.utils.constants import SOURCE_TAG
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.v2.processes.connectors.mongodb import (
    CONNECTOR_TYPE,
    MongoDBAccessConfig,
    MongoDBConnectionConfig,
    MongoDBDownloader,
    MongoDBDownloaderConfig,
    MongoDBIndexer,
    MongoDBIndexerConfig,
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


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@requires_env("MONGODB_URI", "MONGODB_DATABASE")
async def test_mongodb_source(temp_dir: Path):
    env_data = get_env_data()
    indexer_config = MongoDBIndexerConfig()
    download_config = MongoDBDownloaderConfig(download_dir=temp_dir)
    connection_config = MongoDBConnectionConfig(
        access_config=MongoDBAccessConfig(uri=env_data.uri.get_secret_value()),
        database=env_data.database,
        collection=SOURCE_COLLECTION,
    )
    indexer = MongoDBIndexer(connection_config=connection_config, index_config=indexer_config)
    downloader = MongoDBDownloader(
        connection_config=connection_config, download_config=download_config
    )
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=ValidationConfigs(
            test_id=CONNECTOR_TYPE, expected_num_files=4, validate_downloaded_files=True
        ),
    )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
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


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
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


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
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
