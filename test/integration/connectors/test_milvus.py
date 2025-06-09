import json
import time
from pathlib import Path

import docker
import numpy as np
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
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.processes.connectors.milvus import (
    ALLOWED_METADATA_FIELDS,
    CONNECTOR_TYPE,
    MilvusConnectionConfig,
    MilvusUploader,
    MilvusUploaderConfig,
    MilvusUploadStager,
    MilvusUploadStagerConfig,
)

DB_NAME = "test_database"
EXISTENT_COLLECTION_NAME = "test_collection"
COLLECTION_WITHOUT_DYNAMIC_FIELDS = "test_collection_no_dynamic"
NONEXISTENT_COLLECTION_NAME = "nonexistent_collection"
DB_URI = "http://localhost:19530"


def add_fake_embeddings(upload_file: Path, tmp_path: Path) -> Path:
    """
    Reads a JSON file of elements, adds a fake embedding vector to each element,
    and returns the path to the new file.
    """
    with open(upload_file) as f:
        elements = json.load(f)

    for el in elements:
        el["embeddings"] = np.random.rand(384).tolist()

    output_filename = tmp_path / "docs-with-fake-embeddings.json"
    with open(output_filename, "w") as f:
        json.dump(elements, f)
    return output_filename


def get_schema(enable_dynamic_field: bool = True) -> CollectionSchema:
    id_field = FieldSchema(
        name="id",
        dtype=DataType.INT64,
        description="primary field",
        is_primary=True,
        auto_id=True,
    )
    embeddings_field = FieldSchema(
        name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=384
    )
    record_id_field = FieldSchema(
        name="record_id", dtype=DataType.VARCHAR, max_length=64
    )

    schema = CollectionSchema(
        enable_dynamic_field=enable_dynamic_field,
        fields=[
            id_field,
            record_id_field,
            embeddings_field,
        ],
    )

    return schema


def get_index_params() -> IndexParams:
    index_params = IndexParams()
    index_params.add_index(
        field_name="embeddings", index_type="AUTOINDEX", metric_type="COSINE"
    )
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
            database_resp = milvus_client._get_connection().create_database(
                db_name=DB_NAME
            )
            milvus_client.using_database(db_name=DB_NAME)

            print(f"Created database {DB_NAME}: {database_resp}")

            # Create the collection with dynamic fields enabled
            schema = get_schema(enable_dynamic_field=True)
            index_params = get_index_params()
            collection_resp = milvus_client.create_collection(
                collection_name=EXISTENT_COLLECTION_NAME,
                schema=schema,
                index_params=index_params,
            )
            print(f"Created collection {EXISTENT_COLLECTION_NAME}: {collection_resp}")

            # Create a second collection without dynamic fields for testing
            schema_no_dynamic = get_schema(enable_dynamic_field=False)
            collection_resp_no_dynamic = milvus_client.create_collection(
                collection_name=COLLECTION_WITHOUT_DYNAMIC_FIELDS,
                schema=schema_no_dynamic,
                index_params=index_params,
            )
            print(
                f"Created collection {COLLECTION_WITHOUT_DYNAMIC_FIELDS}: \
                {collection_resp_no_dynamic}"
            )

            yield EXISTENT_COLLECTION_NAME
        finally:
            milvus_client.close()


def get_count(client: MilvusClient, collection_name: str = "test_collection") -> int:
    count_field = "count(*)"
    resp = client.query(collection_name=collection_name, output_fields=[count_field])
    return resp[0][count_field]


def validate_count(
    client: MilvusClient,
    expected_count: int,
    collection_name: str = "test_collection",
    retries: int = 10,
    interval: int = 1,
) -> None:
    current_count = get_count(client=client, collection_name=collection_name)
    retry_count = 0
    while current_count != expected_count and retry_count < retries:
        time.sleep(interval)
        current_count = get_count(client=client, collection_name=collection_name)
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
    upload_file_with_embeddings = add_fake_embeddings(upload_file, tmp_path)
    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=str(upload_file), filename=upload_file.stem
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    stager = MilvusUploadStager()
    uploader = MilvusUploader(
        connection_config=MilvusConnectionConfig(uri=DB_URI),
        upload_config=MilvusUploaderConfig(collection_name=collection, db_name=DB_NAME),
    )
    staged_filepath = stager.run(
        elements_filepath=upload_file_with_embeddings,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file_with_embeddings.name,
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


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
async def test_milvus_metadata_storage_with_dynamic_fields(
    upload_file: Path,
    collection: str,
    tmp_path: Path,
):
    """Test that metadata is properly stored when dynamic fields are enabled."""
    upload_file_with_embeddings = add_fake_embeddings(upload_file, tmp_path)
    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=str(upload_file), filename=upload_file.stem
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="metadata_test_file",
    )

    stager = MilvusUploadStager()
    uploader = MilvusUploader(
        connection_config=MilvusConnectionConfig(uri=DB_URI),
        upload_config=MilvusUploaderConfig(collection_name=collection, db_name=DB_NAME),
    )

    # Verify dynamic fields are enabled
    assert (
        uploader.has_dynamic_fields_enabled()
    ), "Collection should have dynamic fields enabled"

    staged_filepath = stager.run(
        elements_filepath=upload_file_with_embeddings,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file_with_embeddings.name,
    )

    # Clean None values from staged data to prevent silent record dropping by Milvus
    with staged_filepath.open() as f:
        staged_data = json.load(f)
    cleaned_data = [{k: v for k, v in item.items() if v is not None} for item in staged_data]
    cleaned_staged_filepath = tmp_path / "cleaned_staged_data.json"
    with cleaned_staged_filepath.open("w") as f:
        json.dump(cleaned_data, f)

    # Load staged data to check what metadata was extracted
    with staged_filepath.open() as f:
        staged_elements = json.load(f)

    # Verify that metadata fields are present in staged data
    sample_element = staged_elements[0]
    metadata_found = []
    for field in ALLOWED_METADATA_FIELDS:
        if field in sample_element:
            metadata_found.append(field)

    assert (
        len(metadata_found) > 0
    ), f"Expected to find metadata fields in staged data.\
   Available keys: {list(sample_element.keys())}"
    print(f"Found metadata fields: {metadata_found}")

    uploader.run(path=cleaned_staged_filepath, file_data=file_data)

    # Query the uploaded data to verify metadata was stored
    with uploader.get_client() as client:
        # Query with specific record ID
        results = client.query(
            collection_name=collection,
            filter=f'record_id == "{file_data.identifier}"',
            output_fields=["*"],  # Get all fields including dynamic fields
        )

        assert len(results) > 0, "Should have results from the uploaded data"

        # Check that metadata fields are present in the results
        sample_result = results[0]
        stored_metadata = []
        for field in metadata_found:
            if field in sample_result:
                stored_metadata.append(field)

        assert (
            len(stored_metadata) > 0
        ), f"Expected metadata fields to be stored in Milvus. \
        Available fields: {list(sample_result.keys())}"
        print(f"Successfully stored metadata fields: {stored_metadata}")

        # Verify filename is specifically stored if present
        if "filename" in stored_metadata:
            assert (
                sample_result["filename"] == upload_file.stem
            ), "Filename should be correctly stored"


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
async def test_milvus_metadata_filtering_without_dynamic_fields(
    upload_file: Path,
    collection: str,
    tmp_path: Path,
):
    """Test that metadata is properly filtered when dynamic fields are not enabled."""
    upload_file_with_embeddings = add_fake_embeddings(upload_file, tmp_path)
    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=str(upload_file), filename=upload_file.stem
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="no_dynamic_test_file",
    )

    stager = MilvusUploadStager()
    uploader = MilvusUploader(
        connection_config=MilvusConnectionConfig(uri=DB_URI),
        upload_config=MilvusUploaderConfig(
            collection_name=COLLECTION_WITHOUT_DYNAMIC_FIELDS, db_name=DB_NAME
        ),
    )

    # Verify dynamic fields are NOT enabled
    assert (
        not uploader.has_dynamic_fields_enabled()
    ), "Collection should NOT have dynamic fields enabled"

    staged_filepath = stager.run(
        elements_filepath=upload_file_with_embeddings,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file_with_embeddings.name,
    )

    # This should not raise an error even though metadata fields are present in staged data
    uploader.run(path=staged_filepath, file_data=file_data)

    # Verify data was uploaded successfully
    with uploader.get_client() as client:
        results = client.query(
            collection_name=COLLECTION_WITHOUT_DYNAMIC_FIELDS,
            filter=f'record_id == "{file_data.identifier}"',
            output_fields=["*"],
        )

        assert len(results) > 0, "Should have results from the uploaded data"
        
        # Verify that only core fields are present (no metadata fields)
        sample_result = results[0]
        core_fields = {'id', 'record_id', 'embeddings'}
        
        # The result should only contain the fields defined in the schema
        assert set(sample_result.keys()) == core_fields, (
            "Unexpected fields found in collection with dynamic fields disabled. "
            f"Expected: {core_fields}, Found: {set(sample_result.keys())}"
        )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_dynamic_fields_detection(collection: str):
    """Test that dynamic field detection works correctly."""
    # Test with dynamic fields enabled
    uploader_with_dynamic = MilvusUploader(
        connection_config=MilvusConnectionConfig(uri=DB_URI),
        upload_config=MilvusUploaderConfig(db_name=DB_NAME, collection_name=collection),
    )
    assert (
        uploader_with_dynamic.has_dynamic_fields_enabled()
    ), "Should detect dynamic fields are enabled"

    # Test with dynamic fields disabled
    uploader_without_dynamic = MilvusUploader(
        connection_config=MilvusConnectionConfig(uri=DB_URI),
        upload_config=MilvusUploaderConfig(
            db_name=DB_NAME, collection_name=COLLECTION_WITHOUT_DYNAMIC_FIELDS
        ),
    )
    assert (
        not uploader_without_dynamic.has_dynamic_fields_enabled()
    ), "Should detect dynamic fields are disabled"


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_metadata_fields_configuration():
    """Test that metadata fields can be configured in the stager."""
    # Test default metadata fields
    stager_default = MilvusUploadStager()
    assert stager_default.upload_stager_config.metadata_fields == list(
        ALLOWED_METADATA_FIELDS
    )

    # Test custom metadata fields
    custom_fields = ["filename", "page_number"]
    stager_custom = MilvusUploadStager(
        upload_stager_config=MilvusUploadStagerConfig(metadata_fields=custom_fields)
    )
    assert stager_custom.upload_stager_config.metadata_fields == custom_fields


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
        upload_config=MilvusUploaderConfig(
            db_name="nonexisting_db", collection_name=collection
        ),
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
