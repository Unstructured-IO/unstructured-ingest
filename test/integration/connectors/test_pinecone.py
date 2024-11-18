import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Generator
from uuid import uuid4

import pytest
from pinecone import Pinecone

from test.integration.utils import requires_env
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.pinecone import (
    CONNECTOR_TYPE,
    PineconeAccessConfig,
    PineconeConnectionConfig,
    PineconeUploader,
    PineconeUploaderConfig,
    PineconeUploadStager,
)

METADATA_BYTES_LIMIT = (
    40960  # 40KB https://docs.pinecone.io/reference/quotas-and-limits#hard-limits
)
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY")
VECTOR_DIMENSION = 384
SPEC = {"serverless": {"cloud": "aws", "region": "us-east-1"}}
ALLOWED_METADATA_FIELD = "text"


@pytest.fixture
def index_name() -> Generator[str, None, None]:
    client = Pinecone(api_key=PINECONE_API_KEY)
    index_name = f"ingest-test-{str(uuid4())[:8]}"
    client.create_index(
        name=index_name,
        dimension=VECTOR_DIMENSION,
        spec=SPEC,
    )
    yield index_name
    client.delete_index(index_name)


@requires_env("PINECONE_API_KEY")
def test_large_metadata(index_name: str, tmp_path: Path, upload_file: Path):
    stager = PineconeUploadStager()
    uploader = PineconeUploader(
        connection_config=PineconeConnectionConfig(
            access_config=PineconeAccessConfig(api_key=PINECONE_API_KEY),
            index_name=index_name,
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

    staged_file = stager.run(large_metadata_upload_file, tmp_path, large_metadata_upload_file.name)
    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=large_metadata_upload_file.name, filename=large_metadata_upload_file.name
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="mock-file-data",
    )
    try:
        uploader.run(staged_file, file_data)
    except DestinationConnectionError as e:
        if (
            re.search(
                re.compile(
                    "Metadata size is \d+ bytes, which exceeds the limit of \d+ bytes per vector"
                ),
                str(e),
            )
            is None
        ):
            raise e
        raise pytest.fail("Upload request failed due to metadata exceeding limits.")

    index = Pinecone(api_key=PINECONE_API_KEY).Index(index_name)
    total_vector_count = index.describe_index_stats()["total_vector_count"]
    attempts = 0
    while total_vector_count == 0 and attempts < 8:
        attempts += 1
        total_vector_count = index.describe_index_stats()["total_vector_count"]
        time.sleep(5)
        logging.info(f"Waiting 5s for index to be updated, attempt {attempts}")

    assert total_vector_count == 1
