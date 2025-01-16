import json
import math
import os
import re
import time
from pathlib import Path
from typing import Generator
from uuid import uuid4

import pytest
from _pytest.fixtures import TopRequest
from pinecone import Pinecone, ServerlessSpec
from pinecone.core.openapi.shared.exceptions import NotFoundException

from test.integration.connectors.utils.constants import DESTINATION_TAG, VECTOR_DB_TAG
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connectors.pinecone import (
    CONNECTOR_TYPE,
    MAX_QUERY_RESULTS,
    PineconeAccessConfig,
    PineconeConnectionConfig,
    PineconeUploader,
    PineconeUploaderConfig,
    PineconeUploadStager,
    PineconeUploadStagerConfig,
)

METADATA_BYTES_LIMIT = (
    40960  # 40KB https://docs.pinecone.io/reference/quotas-and-limits#hard-limits
)
VECTOR_DIMENSION = 384
SPEC = {"serverless": {"cloud": "aws", "region": "us-east-1"}}
ALLOWED_METADATA_FIELD = "text"
API_KEY = "PINECONE_API_KEY"


def get_api_key() -> str:
    api_key = os.getenv(API_KEY, None)
    assert api_key
    return api_key


def wait_for_delete(client: Pinecone, index_name: str, timeout=60, interval=1) -> None:
    start = time.time()
    while True and time.time() - start < timeout:
        try:
            description = client.describe_index(name=index_name)
            logger.info(f"current index status: {description}")
        except NotFoundException:
            return
        time.sleep(interval)

    raise TimeoutError("time out waiting for index to delete")


def wait_for_ready(client: Pinecone, index_name: str, timeout=60, interval=1) -> None:
    def is_ready_status():
        description = client.describe_index(name=index_name)
        status = description["status"]
        return status["ready"]

    start = time.time()
    is_ready = is_ready_status()
    while not is_ready and time.time() - start < timeout:
        time.sleep(interval)
        is_ready = is_ready_status()
    if not is_ready:
        raise TimeoutError("time out waiting for index to be ready")


@pytest.fixture
def pinecone_index() -> Generator[str, None, None]:
    pinecone = Pinecone(api_key=get_api_key())
    random_id = str(uuid4()).split("-")[0]
    index_name = f"ingest-test-{random_id}"
    assert len(index_name) < 45
    logger.info(f"Creating index: {index_name}")
    try:
        pinecone.create_index(
            name=index_name,
            dimension=384,
            metric="cosine",
            spec=ServerlessSpec(
                cloud="aws",
                region="us-east-1",
            ),
            deletion_protection="disabled",
        )
        wait_for_ready(client=pinecone, index_name=index_name)
        yield index_name
    except Exception as e:
        logger.error(f"failed to create index {index_name}: {e}")
    finally:
        try:
            logger.info(f"deleting index: {index_name}")
            pinecone.delete_index(name=index_name)
            wait_for_delete(client=pinecone, index_name=index_name)
        except NotFoundException:
            return


def validate_pinecone_index(
    index_name: str,
    expected_num_of_vectors: int,
    retries=30,
    interval=1,
    namespace: str = "default",
) -> None:
    # Because there's a delay for the index to catch up to the recent writes, add in a retry
    pinecone = Pinecone(api_key=get_api_key())
    index = pinecone.Index(name=index_name, namespace=namespace)
    vector_count = -1
    for i in range(retries):
        index_stats = index.describe_index_stats()
        vector_count = index_stats["total_vector_count"]
        if vector_count == expected_num_of_vectors:
            logger.info(f"expected {expected_num_of_vectors} == vector count {vector_count}")
            break
        logger.info(
            f"retry attempt {i}: expected {expected_num_of_vectors} != vector count {vector_count}"
        )
        time.sleep(interval)
    assert vector_count == expected_num_of_vectors, (
        f"vector count from index ({vector_count}) doesn't "
        f"match expected number: {expected_num_of_vectors}"
    )


@requires_env(API_KEY)
@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
async def test_pinecone_destination(pinecone_index: str, upload_file: Path, temp_dir: Path):

    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="pinecone_mock_id",
    )

    connection_config = PineconeConnectionConfig(
        index_name=pinecone_index,
        access_config=PineconeAccessConfig(api_key=get_api_key()),
    )
    stager_config = PineconeUploadStagerConfig()
    stager = PineconeUploadStager(upload_stager_config=stager_config)
    new_upload_file = stager.run(
        elements_filepath=upload_file,
        output_dir=temp_dir,
        output_filename=upload_file.name,
        file_data=file_data,
    )

    upload_config = PineconeUploaderConfig()
    uploader = PineconeUploader(connection_config=connection_config, upload_config=upload_config)
    uploader.precheck()

    uploader.run(path=new_upload_file, file_data=file_data)
    with new_upload_file.open() as f:
        staged_content = json.load(f)
    expected_num_of_vectors = len(staged_content)
    logger.info("validating first upload")
    validate_pinecone_index(
        index_name=pinecone_index, expected_num_of_vectors=expected_num_of_vectors
    )

    # Rerun uploader and make sure no duplicates exist
    uploader.run(path=new_upload_file, file_data=file_data)
    logger.info("validating second upload")
    validate_pinecone_index(
        index_name=pinecone_index, expected_num_of_vectors=expected_num_of_vectors
    )


@requires_env(API_KEY)
@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@pytest.mark.skip(reason="TODO: get this to work")
async def test_pinecone_destination_large_index(
    pinecone_index: str, upload_file: Path, temp_dir: Path
):
    new_file = temp_dir / "large_file.json"
    with upload_file.open() as f:
        upload_content = json.load(f)

    min_entries = math.ceil((MAX_QUERY_RESULTS * 2) / len(upload_content))
    new_content = (upload_content * min_entries)[: (2 * MAX_QUERY_RESULTS)]
    print(f"Creating large index content with {len(new_content)} records")
    with new_file.open("w") as f:
        json.dump(new_content, f)

    expected_num_of_vectors = len(new_content)
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=new_file.name, filename=new_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="pinecone_mock_id",
    )
    connection_config = PineconeConnectionConfig(
        index_name=pinecone_index,
        access_config=PineconeAccessConfig(api_key=get_api_key()),
    )
    stager_config = PineconeUploadStagerConfig()
    stager = PineconeUploadStager(upload_stager_config=stager_config)
    new_upload_file = stager.run(
        elements_filepath=new_file,
        output_dir=temp_dir,
        output_filename=new_file.name,
        file_data=file_data,
    )

    upload_config = PineconeUploaderConfig()
    uploader = PineconeUploader(connection_config=connection_config, upload_config=upload_config)
    uploader.precheck()

    uploader.run(path=new_upload_file, file_data=file_data)
    validate_pinecone_index(
        index_name=pinecone_index, expected_num_of_vectors=expected_num_of_vectors
    )
    # Rerun uploader and make sure no duplicates exist
    uploader.run(path=new_upload_file, file_data=file_data)
    logger.info("validating second upload")
    validate_pinecone_index(
        index_name=pinecone_index, expected_num_of_vectors=expected_num_of_vectors
    )


@requires_env(API_KEY)
@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
async def test_pinecone_destination_namespace(
    pinecone_index: str, upload_file: Path, temp_dir: Path
):
    """
    tests namespace functionality of destination connector.
    """

    # creates a file data structure.
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="pinecone_mock_id",
    )

    connection_config = PineconeConnectionConfig(
        index_name=pinecone_index,
        access_config=PineconeAccessConfig(api_key=get_api_key()),
    )

    stager_config = PineconeUploadStagerConfig()

    stager = PineconeUploadStager(upload_stager_config=stager_config)
    new_upload_file = stager.run(
        elements_filepath=upload_file,
        output_dir=temp_dir,
        output_filename=upload_file.name,
        file_data=file_data,
    )

    # here add namespace defintion
    upload_config = PineconeUploaderConfig()
    namespace_test_name = "user-1"
    upload_config.namespace = namespace_test_name
    uploader = PineconeUploader(connection_config=connection_config, upload_config=upload_config)
    uploader.precheck()

    uploader.run(path=new_upload_file, file_data=file_data)
    with new_upload_file.open() as f:
        staged_content = json.load(f)
    expected_num_of_vectors = len(staged_content)
    logger.info("validating first upload")
    validate_pinecone_index(
        index_name=pinecone_index,
        expected_num_of_vectors=expected_num_of_vectors,
        namespace=namespace_test_name,
    )

    # Rerun uploader and make sure no duplicates exist
    uploader.run(path=new_upload_file, file_data=file_data)
    logger.info("validating second upload")
    validate_pinecone_index(
        index_name=pinecone_index,
        expected_num_of_vectors=expected_num_of_vectors,
        namespace=namespace_test_name,
    )


@requires_env(API_KEY)
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_large_metadata(pinecone_index: str, tmp_path: Path, upload_file: Path):
    stager = PineconeUploadStager()
    uploader = PineconeUploader(
        connection_config=PineconeConnectionConfig(
            access_config=PineconeAccessConfig(api_key=get_api_key()),
            index_name=pinecone_index,
        ),
        upload_config=PineconeUploaderConfig(),
    )
    large_metadata_upload_file = tmp_path / "mock-upload-file.pdf.json"
    large_metadata = {ALLOWED_METADATA_FIELD: "0" * 2 * METADATA_BYTES_LIMIT}

    with open(upload_file) as file:
        elements = json.load(file)

    with open(large_metadata_upload_file, "w") as file:
        mock_element = elements[0]
        mock_element["metadata"] = large_metadata
        json.dump([mock_element], file)

    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=large_metadata_upload_file.name, filename=large_metadata_upload_file.name
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="mock-file-data",
    )
    staged_file = stager.run(
        elements_filepath=large_metadata_upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=large_metadata_upload_file.name,
    )
    try:
        uploader.run(staged_file, file_data)
    except DestinationConnectionError as e:
        error_line = r"Metadata size is \d+ bytes, which exceeds the limit of \d+ bytes per vector"
        if re.search(re.compile(error_line), str(e)) is None:
            raise e
        raise pytest.fail("Upload request failed due to metadata exceeding limits.")

    validate_pinecone_index(pinecone_index, 1, interval=5)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_pinecone_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = PineconeUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
