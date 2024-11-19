import pytest
from pymilvus import (
    CollectionSchema,
    DataType,
    FieldSchema,
    MilvusClient,
)
from pymilvus.milvus_client import IndexParams

from test.integration.connectors.utils.constants import env_setup_path
from test.integration.connectors.utils.docker_compose import docker_compose_context
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.v2.processes.connectors.milvus import (
    MilvusAccessConfig,
    MilvusConnectionConfig,
    MilvusUploader,
    MilvusUploaderConfig,
)

DOCKER_COMPOSE_PATH = env_setup_path / "milvus"

DATABASE_NAME = "ingest_test_db"
EXISTENT_COLLECTION_NAME = "ingest_test"
NONEXISTENT_COLLECTION_NAME = "nonexistant"
URI = "http://localhost:19530"

SCHEMA = CollectionSchema(
    enable_dynamic_field=True,
    fields=[
        FieldSchema(
            name="id",
            dtype=DataType.INT64,
            description="primary field",
            is_primary=True,
            auto_id=True,
        ),
        FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=384),
    ],
)
INDEX_PARAMS = IndexParams(field_name="embeddings", index_type="AUTOINDEX", metric_type="COSINE")


@pytest.fixture(autouse=True, scope="module")
def _milvus_local_instance():
    with docker_compose_context(DOCKER_COMPOSE_PATH):
        client = MilvusClient(uri=URI)
        client.create_database(DATABASE_NAME)
        client.using_database(DATABASE_NAME)
        client.create_collection(EXISTENT_COLLECTION_NAME, schema=SCHEMA, index_params=INDEX_PARAMS)
        yield
        for collection in client.list_collections():
            client.drop_collection(collection_name=collection)
        client.drop_database(DATABASE_NAME)


def test_precheck_succeeds():
    connection_config = MilvusConnectionConfig(
        access_config=MilvusAccessConfig(),
        uri=URI,
        db_name=DATABASE_NAME,
    )
    uploader = MilvusUploader(
        connection_config=connection_config,
        upload_config=MilvusUploaderConfig(collection_name=EXISTENT_COLLECTION_NAME),
    )
    uploader.precheck()


def test_precheck_fails():
    connection_config = MilvusConnectionConfig(
        access_config=MilvusAccessConfig(),
        uri=URI,
        db_name=DATABASE_NAME,
    )
    uploader = MilvusUploader(
        connection_config=connection_config,
        upload_config=MilvusUploaderConfig(collection_name=NONEXISTENT_COLLECTION_NAME),
    )
    with pytest.raises(
        DestinationConnectionError,
        match=f"Collection '{NONEXISTENT_COLLECTION_NAME}' does not exist",
    ):
        uploader.precheck()
