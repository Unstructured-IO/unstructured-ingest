import json
import time
from pathlib import Path

import docker
import pytest
from _pytest.fixtures import TopRequest
from pymilvus import (
    CollectionSchema,
    DataType,
    FieldSchema,
    MilvusClient,
)
from pymilvus.milvus_client import IndexParams

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
    VECTOR_DB_TAG,
    env_setup_path,
)
from test.integration.connectors.utils.docker import healthcheck_wait
from test.integration.connectors.utils.docker_compose import docker_compose_context
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.milvus import (
    CONNECTOR_TYPE,
    MilvusConnectionConfig,
    MilvusUploader,
    MilvusUploaderConfig,
    MilvusUploadStager,
)

DB_NAME = "test_database"
EXISTENT_COLLECTION_NAME = "test_collection"
NONEXISTENT_COLLECTION_NAME = "nonexistent_collection"
DB_URI = "http://localhost:19530"


def get_schema() -> CollectionSchema:
    id_field = FieldSchema(
        name="id", dtype=DataType.INT64, description="primary field", is_primary=True, auto_id=True
    )
    embeddings_field = FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=384)
    record_id_field = FieldSchema(name="record_id", dtype=DataType.VARCHAR, max_length=64)

    schema = CollectionSchema(
        enable_dynamic_field=True,
        fields=[
            id_field,
            record_id_field,
            embeddings_field,
        ],
    )

    return schema


def get_index_params() -> IndexParams:
    index_params = IndexParams()
    index_params.add_index(field_name="embeddings", index_type="AUTOINDEX", metric_type="COSINE")
    index_params.add_index(field_name="record_id", index_type="Trie")
    return index_params


# NOTE: Precheck tests are read-only so they don't interfere with destination test,
# using scope="module" we can limit number of times the docker-compose has to be run
@pytest.fixture(scope="module")
def collection():
    docker_client = docker.from_env()
    with docker_compose_context(docker_compose_path=env_setup_path / "milvus"):
        milvus_container = docker_client.containers.get("milvus-standalone")
        healthcheck_wait(container=milvus_container)
        milvus_client = MilvusClient(uri=DB_URI)
        try:
            # Create the database
            database_resp = milvus_client._get_connection().create_database(db_name=DB_NAME)
            milvus_client.using_database(db_name=DB_NAME)

            print(f"Created database {DB_NAME}: {database_resp}")

            # Create the collection
            schema = get_schema()
            index_params = get_index_params()
            collection_resp = milvus_client.create_collection(
                collection_name=EXISTENT_COLLECTION_NAME, schema=schema, index_params=index_params
            )
            print(f"Created collection {EXISTENT_COLLECTION_NAME}: {collection_resp}")
            yield EXISTENT_COLLECTION_NAME
        finally:
            milvus_client.close()


def get_count(client: MilvusClient) -> int:
    count_field = "count(*)"
    resp = client.query(collection_name="test_collection", output_fields=[count_field])
    return resp[0][count_field]


def validate_count(
    client: MilvusClient, expected_count: int, retries: int = 10, interval: int = 1
) -> None:
    current_count = get_count(client=client)
    retry_count = 0
    while current_count != expected_count and retry_count < retries:
        time.sleep(interval)
        current_count = get_count(client=client)
        retry_count += 1
    assert current_count == expected_count, (
        f"Expected count ({expected_count}) doesn't match how "
        f"much came back from collection: {current_count}"
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
async def test_milvus_destination(
    upload_file: Path,
    collection: str,
    tmp_path: Path,
):
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    stager = MilvusUploadStager()
    uploader = MilvusUploader(
        connection_config=MilvusConnectionConfig(uri=DB_URI),
        upload_config=MilvusUploaderConfig(collection_name=collection, db_name=DB_NAME),
    )
    staged_filepath = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )
    uploader.precheck()
    uploader.run(path=staged_filepath, file_data=file_data)

    # Run validation
    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    expected_count = len(staged_elements)
    with uploader.get_client() as client:
        validate_count(client=client, expected_count=expected_count)

    # Rerun and make sure the same documents get updated
    uploader.run(path=staged_filepath, file_data=file_data)
    with uploader.get_client() as client:
        validate_count(client=client, expected_count=expected_count)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_precheck_succeeds(collection: str):
    uploader = MilvusUploader(
        connection_config=MilvusConnectionConfig(uri=DB_URI),
        upload_config=MilvusUploaderConfig(db_name=DB_NAME, collection_name=collection),
    )
    uploader.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_precheck_fails_on_nonexistent_collection(collection: str):
    uploader = MilvusUploader(
        connection_config=MilvusConnectionConfig(uri=DB_URI),
        upload_config=MilvusUploaderConfig(
            db_name=DB_NAME, collection_name=NONEXISTENT_COLLECTION_NAME
        ),
    )
    with pytest.raises(
        DestinationConnectionError,
        match=f"Collection '{NONEXISTENT_COLLECTION_NAME}' does not exist",
    ):
        uploader.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_precheck_fails_on_nonexisting_db(collection: str):
    uploader = MilvusUploader(
        connection_config=MilvusConnectionConfig(uri=DB_URI),
        upload_config=MilvusUploaderConfig(db_name="nonexisting_db", collection_name=collection),
    )
    with pytest.raises(
        DestinationConnectionError,
        match="database not found",
    ):
        uploader.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_milvus_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = MilvusUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
