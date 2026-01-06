import contextlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest
from _pytest.fixtures import TopRequest
from astrapy import Collection
from astrapy import DataAPIClient as AstraDBClient
from astrapy.info import CollectionDefinition

from test.integration.connectors.utils.constants import DESTINATION_TAG, SOURCE_TAG, VECTOR_DB_TAG
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
    source_filedata_display_name_set_check,
)
from test.integration.utils import requires_env
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.astradb import (
    CONNECTOR_TYPE,
    AstraDBAccessConfig,
    AstraDBConnectionConfig,
    AstraDBDownloader,
    AstraDBDownloaderConfig,
    AstraDBIndexer,
    AstraDBIndexerConfig,
    AstraDBUploader,
    AstraDBUploaderConfig,
    AstraDBUploadStager,
    AstraDBUploadStagerConfig,
    DestinationConnectionError,
    SourceConnectionError,
)

EXISTENT_COLLECTION_NAME = "ingest_test_src"
NONEXISTENT_COLLECTION_NAME = "nonexistant"


@pytest.fixture
def connection_config() -> AstraDBConnectionConfig:
    return AstraDBConnectionConfig(
        access_config=AstraDBAccessConfig(
            token=os.environ["ASTRA_DB_APPLICATION_TOKEN"],
            api_endpoint=os.environ["ASTRA_DB_API_ENDPOINT"],
        )
    )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, VECTOR_DB_TAG)
@requires_env("ASTRA_DB_APPLICATION_TOKEN", "ASTRA_DB_API_ENDPOINT")
def test_precheck_succeeds_indexer(connection_config: AstraDBConnectionConfig):
    indexer = AstraDBIndexer(
        connection_config=connection_config,
        index_config=AstraDBIndexerConfig(collection_name=EXISTENT_COLLECTION_NAME),
    )
    indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@requires_env("ASTRA_DB_APPLICATION_TOKEN", "ASTRA_DB_API_ENDPOINT")
def test_precheck_succeeds_uploader(connection_config: AstraDBConnectionConfig):
    uploader = AstraDBUploader(
        connection_config=connection_config,
        upload_config=AstraDBUploaderConfig(collection_name=EXISTENT_COLLECTION_NAME),
    )
    uploader.precheck()

    uploader2 = AstraDBUploader(
        connection_config=connection_config,
        upload_config=AstraDBUploaderConfig(),
    )
    uploader2.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, VECTOR_DB_TAG)
@requires_env("ASTRA_DB_APPLICATION_TOKEN", "ASTRA_DB_API_ENDPOINT")
def test_precheck_fails_indexer(connection_config: AstraDBConnectionConfig):
    indexer = AstraDBIndexer(
        connection_config=connection_config,
        index_config=AstraDBIndexerConfig(collection_name=NONEXISTENT_COLLECTION_NAME),
    )
    with pytest.raises(expected_exception=SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@requires_env("ASTRA_DB_APPLICATION_TOKEN", "ASTRA_DB_API_ENDPOINT")
def test_precheck_fails_uploader(connection_config: AstraDBConnectionConfig):
    uploader = AstraDBUploader(
        connection_config=connection_config,
        upload_config=AstraDBUploaderConfig(collection_name=NONEXISTENT_COLLECTION_NAME),
    )
    with pytest.raises(expected_exception=DestinationConnectionError):
        uploader.precheck()


@dataclass(frozen=True)
class EnvData:
    api_endpoint: str
    token: str


def get_env_data() -> EnvData:
    return EnvData(
        api_endpoint=os.environ["ASTRA_DB_API_ENDPOINT"],
        token=os.environ["ASTRA_DB_APPLICATION_TOKEN"],
    )


@pytest.fixture
def collection(upload_file: Path) -> Collection:
    random_id = str(uuid4())[:8]
    collection_name = f"utic_test_{random_id}"
    with upload_file.open("r") as upload_fp:
        upload_data = json.load(upload_fp)
    first_content = upload_data[0]
    embeddings = first_content["embeddings"]
    embedding_dimension = len(embeddings)
    my_client = AstraDBClient()
    env_data = get_env_data()
    astra_db = my_client.get_database(
        api_endpoint=env_data.api_endpoint,
        token=env_data.token,
    )
    collection = astra_db.create_collection(
        collection_name,
        definition=CollectionDefinition.builder()
        .set_vector_dimension(dimension=embedding_dimension)
        .build(),
    )
    try:
        yield collection
    finally:
        astra_db.drop_collection(collection.name)


@pytest.fixture
def large_upload_collection(tmp_path: Path) -> Collection:
    """Create a collection for testing large uploads with concurrency limits."""
    random_id = str(uuid4())[:8]
    collection_name = f"utic_test_large_{random_id}"
    # Use a standard embedding dimension (1536 for OpenAI text-embedding-3-small)
    embedding_dimension = 1536
    my_client = AstraDBClient()
    env_data = get_env_data()
    astra_db = my_client.get_database(
        api_endpoint=env_data.api_endpoint,
        token=env_data.token,
    )
    collection = astra_db.create_collection(
        collection_name,
        definition=CollectionDefinition.builder()
        .set_vector_dimension(dimension=embedding_dimension)
        .build(),
    )
    try:
        yield collection
    finally:
        astra_db.drop_collection(collection.name)


@pytest.fixture
def large_upload_file(tmp_path: Path, upload_file_ndjson: Path) -> Path:
    """Create a large upload file by duplicating elements from the test file."""
    # Read the source file
    with upload_file_ndjson.open("r") as f:
        source_elements = [json.loads(line) for line in f if line.strip()]

    if not source_elements:
        pytest.skip("No elements found in source file")

    # Use the first element as a template
    template_element = source_elements[0]

    # Create a large file with 1000 elements (enough to test concurrency but not too slow)
    # With batch_size=20, this creates 50 batches
    num_elements = 1000
    large_file = tmp_path / "large_upload.ndjson"

    with large_file.open("w") as f:
        for i in range(num_elements):
            # Create a copy with unique element_id
            new_element = json.loads(json.dumps(template_element))
            new_element["element_id"] = str(uuid4()).replace("-", "")
            # Modify text to make each element unique
            new_element["text"] = f"{template_element.get('text', '')} [duplicate {i}]"
            f.write(json.dumps(new_element, ensure_ascii=False) + "\n")

    return large_file


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, VECTOR_DB_TAG)
@requires_env("ASTRA_DB_API_ENDPOINT", "ASTRA_DB_APPLICATION_TOKEN")
async def test_astra_search_source(
    tmp_path: Path,
):
    env_data = get_env_data()
    collection_name = "ingest_test_src"
    connection_config = AstraDBConnectionConfig(
        access_config=AstraDBAccessConfig(token=env_data.token, api_endpoint=env_data.api_endpoint)
    )
    indexer = AstraDBIndexer(
        index_config=AstraDBIndexerConfig(
            collection_name=collection_name,
        ),
        connection_config=connection_config,
    )
    downloader = AstraDBDownloader(
        connection_config=connection_config,
        download_config=AstraDBDownloaderConfig(download_dir=tmp_path),
    )

    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id=CONNECTOR_TYPE,
            expected_num_files=5,
            expected_number_indexed_file_data=1,
            validate_downloaded_files=True,
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
            exclude_fields_extend=["display_name"],  # includes dynamic ids, might change
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@requires_env("ASTRA_DB_API_ENDPOINT", "ASTRA_DB_APPLICATION_TOKEN")
async def test_astra_search_destination(
    upload_file: Path,
    collection: Collection,
    tmp_path: Path,
):
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    stager = AstraDBUploadStager()
    env_data = get_env_data()
    uploader = AstraDBUploader(
        connection_config=AstraDBConnectionConfig(
            access_config=AstraDBAccessConfig(
                api_endpoint=env_data.api_endpoint, token=env_data.token
            ),
        ),
        upload_config=AstraDBUploaderConfig(collection_name=collection.name),
    )
    staged_filepath = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )
    uploader.precheck()
    await uploader.run_async(path=staged_filepath, file_data=file_data)

    # Run validation
    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    expected_count = len(staged_elements)
    current_count = collection.count_documents(filter={}, upper_bound=expected_count * 2)
    assert current_count == expected_count, (
        f"Expected count ({expected_count}) doesn't match how "
        f"much came back from collection: {current_count}"
    )

    # Rerun and make sure the same documents get updated
    await uploader.run_async(path=staged_filepath, file_data=file_data)
    current_count = collection.count_documents(filter={}, upper_bound=expected_count * 2)
    assert current_count == expected_count, (
        f"Expected count ({expected_count}) doesn't match how "
        f"much came back from collection: {current_count}"
    )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@requires_env("ASTRA_DB_API_ENDPOINT", "ASTRA_DB_APPLICATION_TOKEN")
def test_astra_create_destination():
    env_data = get_env_data()
    connection_config = AstraDBConnectionConfig(
        access_config=AstraDBAccessConfig(api_endpoint=env_data.api_endpoint, token=env_data.token),
    )
    uploader = AstraDBUploader(
        connection_config=connection_config,
        upload_config=AstraDBUploaderConfig(),
    )
    collection_name = "system_created-123"
    formatted_collection_name = "system_created_123"

    client = AstraDBClient()
    db = client.get_database(api_endpoint=env_data.api_endpoint, token=env_data.token)
    with contextlib.suppress(Exception):
        # drop collection before trying to create it
        db.drop_collection(formatted_collection_name)

    created = uploader.create_destination(destination_name=collection_name, vector_length=3072)
    assert created
    assert uploader.upload_config.collection_name == formatted_collection_name

    created = uploader.create_destination(destination_name=collection_name, vector_length=3072)
    assert not created

    # cleanup
    db.drop_collection(formatted_collection_name)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_astra_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = AstraDBUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_astra_stager_flatten_metadata(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    stager_config = AstraDBUploadStagerConfig(flatten_metadata=True)
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = AstraDBUploadStager(upload_stager_config=stager_config)
    stager_validation(
        configs=StagerValidationConfigs(
            test_id=CONNECTOR_TYPE, expected_count=22, expected_folder="stager_flatten_metadata"
        ),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@requires_env("ASTRA_DB_API_ENDPOINT", "ASTRA_DB_APPLICATION_TOKEN")
async def test_astra_large_upload_with_concurrency_limit(
    large_upload_file: Path,
    large_upload_collection: Collection,
    tmp_path: Path,
):
    """Test that large uploads work correctly with concurrency limits.

    This test verifies that:
    1. Large uploads (1000+ elements) complete successfully with concurrency limits
    2. All elements are uploaded correctly
    3. The concurrency limit prevents timeouts
    """
    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=large_upload_file.name, filename=large_upload_file.name
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="large upload test",
    )
    stager = AstraDBUploadStager()
    env_data = get_env_data()

    # Configure uploader with low concurrency limit to test the fix
    uploader = AstraDBUploader(
        connection_config=AstraDBConnectionConfig(
            access_config=AstraDBAccessConfig(
                api_endpoint=env_data.api_endpoint, token=env_data.token
            ),
        ),
        upload_config=AstraDBUploaderConfig(
            collection_name=large_upload_collection.name,
            batch_size=20,  # Small batch size to create many batches
            max_concurrent_batches=5,  # Low concurrency limit to prevent timeouts
        ),
    )

    # Stage the large file
    staged_filepath = stager.run(
        elements_filepath=large_upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=large_upload_file.name,
    )

    uploader.precheck()

    # Upload with concurrency limit - this should complete without timeout
    await uploader.run_async(path=staged_filepath, file_data=file_data)

    # Verify all elements were uploaded
    # The stager outputs ndjson format (one JSON object per line)
    with staged_filepath.open() as f:
        staged_elements = [json.loads(line) for line in f if line.strip()]

    expected_count = len(staged_elements)
    # Use a reasonable upper bound for counting
    current_count = large_upload_collection.count_documents(
        filter={}, upper_bound=expected_count * 2
    )

    assert current_count == expected_count, (
        f"Expected {expected_count} documents but found {current_count}. "
        f"This indicates the upload may have failed or been incomplete."
    )

    # Verify we can query the documents back
    # Sample a few element_ids from the staged elements to verify they exist
    sample_elements = staged_elements[:5]
    for staged_elem in sample_elements:
        element_id = staged_elem.get("metadata", {}).get("element_id")
        if element_id:
            # Query by element_id in metadata
            results = list(
                large_upload_collection.find(
                    filter={"metadata.element_id": {"$eq": element_id}}, limit=1
                )
            )
            assert len(results) == 1, (
                f"Element with element_id {element_id} not found in collection. "
                f"Found {len(results)} results."
            )
