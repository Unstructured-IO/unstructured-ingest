import json
import logging
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
    CONNECTOR_TYPE,
    MilvusConnectionConfig,
    MilvusUploader,
    MilvusUploaderConfig,
    MilvusUploadStager,
)

logger = logging.getLogger(__name__)

DB_NAME = "test_database"
EXISTENT_COLLECTION_NAME = "test_collection"
COLLECTION_WITHOUT_DYNAMIC_FIELDS = "test_collection_no_dynamic"
NONEXISTENT_COLLECTION_NAME = "nonexistent_collection"
DB_URI = "http://localhost:19530"


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def get_printable_sample(element: dict) -> dict:
    """Creates a copy of an element and truncates embeddings for printing."""
    sample = element.copy()
    if "embeddings" in sample and isinstance(sample["embeddings"], (list, np.ndarray)):
        embeddings = np.asarray(sample["embeddings"])
        sample["embeddings"] = f"first 5: {embeddings[:5].tolist()}..."
    return sample


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
            if DB_NAME not in milvus_client.list_databases():
                database_resp = milvus_client._get_connection().create_database(
                    db_name=DB_NAME,
                )
                logger.debug(f"Created database {DB_NAME}: {database_resp}")
            milvus_client.using_database(db_name=DB_NAME)

            # Create the collection with dynamic fields enabled
            schema = get_schema(enable_dynamic_field=True)
            index_params = get_index_params()
            if milvus_client.has_collection(collection_name=EXISTENT_COLLECTION_NAME):
                milvus_client.drop_collection(collection_name=EXISTENT_COLLECTION_NAME)
            collection_resp = milvus_client.create_collection(
                collection_name=EXISTENT_COLLECTION_NAME,
                schema=schema,
                index_params=index_params,
            )
            logger.debug(f"Created collection {EXISTENT_COLLECTION_NAME}: {collection_resp}")
            desc_dynamic = milvus_client.describe_collection(
                collection_name=EXISTENT_COLLECTION_NAME,
            )
            logger.debug(f"Post-creation description for '{EXISTENT_COLLECTION_NAME}':")
            logger.debug(json.dumps(desc_dynamic, indent=2, cls=NpEncoder))

            # Create a second collection without dynamic fields for testing
            schema_no_dynamic = get_schema(enable_dynamic_field=False)
            if milvus_client.has_collection(
                collection_name=COLLECTION_WITHOUT_DYNAMIC_FIELDS,
            ):
                milvus_client.drop_collection(
                    collection_name=COLLECTION_WITHOUT_DYNAMIC_FIELDS,
                )
            collection_resp_no_dynamic = milvus_client.create_collection(
                collection_name=COLLECTION_WITHOUT_DYNAMIC_FIELDS,
                schema=schema_no_dynamic,
                index_params=index_params,
            )
            logger.debug(
                f"Created collection {COLLECTION_WITHOUT_DYNAMIC_FIELDS}: "
                f"{collection_resp_no_dynamic}",
            )
            desc_no_dynamic = milvus_client.describe_collection(
                collection_name=COLLECTION_WITHOUT_DYNAMIC_FIELDS,
            )
            logger.debug(f"Post-creation description for '{COLLECTION_WITHOUT_DYNAMIC_FIELDS}':")
            logger.debug(json.dumps(desc_no_dynamic, indent=2, cls=NpEncoder))

            yield EXISTENT_COLLECTION_NAME
        finally:
            milvus_client.close()


def get_count(client: MilvusClient, collection_name: str = "test_collection") -> int:
    count_field = "count(*)"
    resp = client.query(collection_name=collection_name, output_fields=[count_field])
    return resp[0][count_field]


def get_count_for_id(client: MilvusClient, collection_name: str, record_id: str) -> int:
    count_field = "count(*)"
    resp = client.query(
        collection_name=collection_name,
        filter=f'record_id == "{record_id}"',
        output_fields=[count_field],
    )
    # The response can be empty if no records match
    if not resp or count_field not in resp[0]:
        return 0
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


def validate_count_for_id(
    client: MilvusClient,
    expected_count: int,
    collection_name: str,
    record_id: str,
    retries: int = 10,
    interval: int = 1,
) -> None:
    current_count = get_count_for_id(
        client=client, collection_name=collection_name, record_id=record_id
    )
    retry_count = 0
    while current_count != expected_count and retry_count < retries:
        time.sleep(interval)
        current_count = get_count_for_id(
            client=client, collection_name=collection_name, record_id=record_id
        )
        retry_count += 1
    assert current_count == expected_count, (
        f"Expected count for record_id '{record_id}' ({expected_count}) doesn't match "
        f"what came back from collection: {current_count}"
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
async def test_milvus_destination(
    upload_file: Path,
    collection: str,
    tmp_path: Path,
):
    logger.debug("\n--- Running test_milvus_destination ---")
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
    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    logger.debug(f"Number of staged elements: {len(staged_elements)}")
    if staged_elements:
        logger.debug("Sample staged element for test_milvus_destination:")
        logger.debug(json.dumps(get_printable_sample(staged_elements[0]), indent=2))
    uploader.precheck()
    logger.debug("Running uploader for the first time...")
    uploader.run(path=staged_filepath, file_data=file_data)
    logger.debug("First uploader run finished.")

    # Run validation
    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    expected_count = len(staged_elements)
    with uploader.get_client() as client:
        logger.debug(f"Validating count, expecting: {expected_count}")
        validate_count_for_id(
            client=client,
            expected_count=expected_count,
            collection_name=collection,
            record_id=file_data.identifier,
        )
        logger.debug("Count validation successful.")

    # Rerun and make sure the same documents get updated
    logger.debug("Running uploader for the second time (rerun)...")
    uploader.run(path=staged_filepath, file_data=file_data)
    logger.debug("Second uploader run finished.")
    with uploader.get_client() as client:
        logger.debug(f"Validating count on rerun, expecting: {expected_count}")
        validate_count_for_id(
            client=client,
            expected_count=expected_count,
            collection_name=collection,
            record_id=file_data.identifier,
        )
        logger.debug("Count validation on rerun successful.")


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
async def test_milvus_metadata_storage_with_dynamic_fields(
    upload_file: Path,
    collection: str,
    tmp_path: Path,
):
    """Test that metadata is properly stored when dynamic fields are enabled."""
    logger.debug("\n--- Running test_milvus_metadata_storage_with_dynamic_fields ---")
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

    # Load staged data to check what metadata was extracted
    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    logger.debug(f"Number of staged elements: {len(staged_elements)}")
    if staged_elements:
        logger.debug("Sample staged element before upload:")
        logger.debug(json.dumps(get_printable_sample(staged_elements[0]), indent=2))

    # Verify that metadata fields are present in staged data
    sample_element = staged_elements[0]
    expected_fields = ["filename", "filetype", "languages"]
    metadata_found = [field for field in expected_fields if field in sample_element]

    assert len(metadata_found) == len(expected_fields), (
        "Expected to find metadata fields in staged data. "
        f"Found: {metadata_found}, Available keys: {list(sample_element.keys())}"
    )
    logger.debug(f"Found metadata fields: {metadata_found}")

    logger.debug("Running uploader...")
    uploader.run(path=staged_filepath, file_data=file_data)
    logger.debug("Uploader finished.")

    # Query the uploaded data to verify metadata was stored
    with uploader.get_client() as client:
        validate_count_for_id(
            client=client,
            expected_count=len(staged_elements),
            collection_name=collection,
            record_id=file_data.identifier,
        )
        # Query with specific record ID
        logger.debug(f"Querying collection '{collection}' for record_id '{file_data.identifier}'")
        results = client.query(
            collection_name=collection,
            filter=f'record_id == "{file_data.identifier}"',
            output_fields=["*"],  # Get all fields including dynamic fields
        )
        logger.debug(f"Query returned {len(results)} results.")
        if results:
            logger.debug("Sample query result:")
            logger.debug(json.dumps(get_printable_sample(results[0]), indent=2, cls=NpEncoder))

        assert len(results) > 0, "Should have results from the uploaded data"

        # Check that metadata fields are present in the results
        sample_result = results[0]
        stored_metadata = []
        for field in metadata_found:
            if field in sample_result:
                stored_metadata.append(field)

        assert len(stored_metadata) > 0, (
            "Expected metadata fields to be stored in Milvus. "
            f"Available fields: {list(sample_result.keys())}"
        )
        logger.debug(f"Successfully stored metadata fields: {stored_metadata}")

        # Verify filename is specifically stored if present
        if "filename" in stored_metadata:
            assert (
                sample_result["filename"] == upload_file.name
            ), "Filename should be correctly stored"


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
async def test_milvus_metadata_filtering_without_dynamic_fields(
    upload_file: Path,
    collection: str,
    tmp_path: Path,
):
    """Test that metadata is properly filtered when dynamic fields are not enabled."""
    logger.debug("\n--- Running test_milvus_metadata_filtering_without_dynamic_fields ---")
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

    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    logger.debug(f"Number of staged elements: {len(staged_elements)}")
    if staged_elements:
        logger.debug("Sample staged element before upload:")
        logger.debug(json.dumps(get_printable_sample(staged_elements[0]), indent=2))

    # This should not raise an error even though metadata fields are present in staged data
    logger.debug("Running uploader...")
    uploader.run(path=staged_filepath, file_data=file_data)
    logger.debug("Uploader finished.")

    # Verify data was uploaded successfully
    with uploader.get_client() as client:
        validate_count_for_id(
            client=client,
            expected_count=len(staged_elements),
            collection_name=COLLECTION_WITHOUT_DYNAMIC_FIELDS,
            record_id=file_data.identifier,
        )
        logger.debug(
            f"Querying collection '{COLLECTION_WITHOUT_DYNAMIC_FIELDS}' "
            f"for record_id '{file_data.identifier}'"
        )
        results = client.query(
            collection_name=COLLECTION_WITHOUT_DYNAMIC_FIELDS,
            filter=f'record_id == "{file_data.identifier}"',
            output_fields=["*"],
        )

        logger.debug(f"Query returned {len(results)} results.")
        if results:
            logger.debug("Sample query result:")
            logger.debug(json.dumps(get_printable_sample(results[0]), indent=2, cls=NpEncoder))
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
