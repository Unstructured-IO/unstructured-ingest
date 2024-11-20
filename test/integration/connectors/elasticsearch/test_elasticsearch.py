import json
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from elasticsearch.helpers import bulk

import pandas as pd
import pytest
from elasticsearch import Elasticsearch as ElasticsearchClient
from test.integration.connectors.utils.constants import DESTINATION_TAG, SOURCE_TAG
from test.integration.connectors.utils.docker import HealthCheck, container_context
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
)
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.elasticsearch.elasticsearch import (
    CONNECTOR_TYPE,
    ElasticsearchAccessConfig,
    ElasticsearchConnectionConfig,
    ElasticsearchDownloader,
    ElasticsearchDownloaderConfig,
    ElasticsearchIndexer,
    ElasticsearchIndexerConfig,
    ElasticsearchUploader,
    ElasticsearchUploaderConfig,
    ElasticsearchUploadStager,
    ElasticsearchUploadStagerConfig,
)

SOURCE_INDEX_NAME = "movies"
DESTINATION_INDEX_NAME = "elements"
ES_USERNAME = "elastic"
ES_PASSWORD = "elastic_password"

@contextmanager
def get_client() -> Generator[ElasticsearchClient, None, None]:
    with ElasticsearchClient(
        hosts="http://localhost:9200",
        basic_auth=(ES_USERNAME, ES_PASSWORD),
        request_timeout=30
    ) as client:
        yield client

def form_elasticsearch_doc_dict(i, csv_row):
    return {
        "_index": SOURCE_INDEX_NAME,
        "_id": i,
        "_source": {
            "title": csv_row["Title"],
            "ethnicity": csv_row["Origin/Ethnicity"],
            "director": csv_row["Director"],
            "cast": csv_row["Cast"],
            "genre": csv_row["Genre"],
            "plot": csv_row["Plot"],
            "year": csv_row["Release Year"],
            "wiki_page": csv_row["Wiki Page"],
        },
    }

def dataframe_to_upload_data(df: pd.DataFrame) -> list[dict]:
    upload_data = []
    for index, row in df.iterrows():
        upload_data.append(form_elasticsearch_doc_dict(index, row))
    return upload_data

def get_index_count(client: ElasticsearchClient, index_name: str) -> int:
    count_resp = client.cat.count(index=index_name, format="json")
    return int(count_resp[0]["count"])

def seed_source_db(df: pd.DataFrame):
    mapping = {
        "properties": {
            "title": {"type": "text", "analyzer": "english"},
            "ethnicity": {"type": "text", "analyzer": "standard"},
            "director": {"type": "text", "analyzer": "standard"},
            "cast": {"type": "text", "analyzer": "standard"},
            "genre": {"type": "text", "analyzer": "standard"},
            "plot": {"type": "text", "analyzer": "english"},
            "year": {"type": "integer"},
            "wiki_page": {"type": "keyword"},
        },
    }
    # seed content
    with get_client() as client:
        client.indices.create(index=SOURCE_INDEX_NAME, mappings=mapping)
        upload_data = dataframe_to_upload_data(df=df)
        bulk(client, upload_data)
        client.indices.refresh(index=SOURCE_INDEX_NAME)
        count = get_index_count(client, SOURCE_INDEX_NAME)
        print(f"seeded {SOURCE_INDEX_NAME} index with {count} records")

@pytest.fixture
def source_index(movies_dataframe: pd.DataFrame) -> str:
    with container_context(
        image="docker.elastic.co/elasticsearch/elasticsearch:8.7.0",
        ports={9200: 9200, 9300: 9300},
        environment={"discovery.type": "single-node", "xpack.security.enabled": True, "ELASTIC_PASSWORD": ES_PASSWORD, "ELASTIC_USER": ES_USERNAME},
        healthcheck=HealthCheck(
            test='curl --silent --fail -u ${ELASTIC_USER}:${ELASTIC_PASSWORD} localhost:9200/_cluster/health || exit 1',  # noqa: E501
            interval=1,
            start_period=5
        ),
    ):
        seed_source_db(df=movies_dataframe)
        yield SOURCE_INDEX_NAME

@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
async def test_elasticsearch_source(source_index: str, movies_dataframe: pd.DataFrame):
    indexer_config = ElasticsearchIndexerConfig(index_name=source_index)
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        connection_config = ElasticsearchConnectionConfig(
            access_config=ElasticsearchAccessConfig(password=ES_PASSWORD),
            username=ES_USERNAME,
            hosts=["http://localhost:9200"],
        )
        download_config = ElasticsearchDownloaderConfig(download_dir=tempdir_path)
        indexer = ElasticsearchIndexer(
            connection_config=connection_config, index_config=indexer_config
        )
        downloader = ElasticsearchDownloader(
            connection_config=connection_config, download_config=download_config
        )
        expected_num_files = len(movies_dataframe)
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=ValidationConfigs(
                test_id=CONNECTOR_TYPE,
                expected_num_files=expected_num_files,
                expected_number_indexed_file_data=1,
                validate_downloaded_files=True,
            ),
        )
        
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
def test_opensearch_source_precheck_fail_no_cluster():
    indexer_config = ElasticsearchIndexerConfig(index_name="index")

    connection_config = ElasticsearchConnectionConfig(
        access_config=ElasticsearchAccessConfig(password=ES_PASSWORD),
        username=ES_USERNAME,
        hosts=["http://localhost:9200"],
    )
    indexer = ElasticsearchIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
def test_opensearch_source_precheck_fail_no_index(source_index: str):
    indexer_config = ElasticsearchIndexerConfig(index_name="index")

    connection_config = ElasticsearchConnectionConfig(
        access_config=ElasticsearchAccessConfig(password=ES_PASSWORD),
        username=ES_USERNAME,
        hosts=["http://localhost:9200"],
    )
    indexer = ElasticsearchIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()
