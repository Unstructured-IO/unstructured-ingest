import json
import os
import time
from pathlib import Path
from uuid import uuid4

import pytest
from pinecone import Pinecone, ServerlessSpec
from pinecone.core.openapi.shared.exceptions import NotFoundException

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connectors.pinecone import (
    CONNECTOR_TYPE,
    PineconeAccessConfig,
    PineconeConnectionConfig,
    PineconeUploader,
    PineconeUploaderConfig,
    PineconeUploadStager,
    PineconeUploadStagerConfig,
)

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
def pinecone_index() -> str:
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
    index_name: str, expected_num_of_vectors: int, retries=30, interval=1
) -> None:
    # Because there's a delay for the index to catch up to the recent writes, add in a retry
    pinecone = Pinecone(api_key=get_api_key())
    index = pinecone.Index(name=index_name)
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
    assert vector_count == expected_num_of_vectors


@requires_env(API_KEY)
@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
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

    if uploader.is_async():
        await uploader.run_async(path=new_upload_file, file_data=file_data)
    else:
        uploader.run(path=new_upload_file, file_data=file_data)
    with new_upload_file.open() as f:
        staged_content = json.load(f)
    expected_num_of_vectors = len(staged_content)
    logger.info("validating first upload")
    validate_pinecone_index(
        index_name=pinecone_index, expected_num_of_vectors=expected_num_of_vectors
    )

    # Rerun uploader and make sure no duplicates exist
    if uploader.is_async():
        await uploader.run_async(path=new_upload_file, file_data=file_data)
    else:
        uploader.run(path=new_upload_file, file_data=file_data)
    logger.info("validating second upload")
    validate_pinecone_index(
        index_name=pinecone_index, expected_num_of_vectors=expected_num_of_vectors
    )
