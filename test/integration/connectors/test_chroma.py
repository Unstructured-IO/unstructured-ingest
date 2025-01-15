import json
from pathlib import Path

import chromadb
import pytest
from _pytest.fixtures import TopRequest

from test.integration.connectors.utils.constants import DESTINATION_TAG, VECTOR_DB_TAG
from test.integration.connectors.utils.docker import HealthCheck, container_context
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.chroma import (
    CONNECTOR_TYPE,
    ChromaConnectionConfig,
    ChromaUploader,
    ChromaUploaderConfig,
    ChromaUploadStager,
    ChromaUploadStagerConfig,
)


@pytest.fixture
def chroma_instance():
    with container_context(
        image="chromadb/chroma:0.6.2",
        ports={8000: 8000},
        name="chroma_int_test",
        healthcheck=HealthCheck(
            interval=5,
            timeout=10,
            retries=3,
            test="timeout 10s bash -c ':> /dev/tcp/127.0.0.1/8000' || exit 1",
        ),
    ) as ctx:
        yield ctx


def validate_collection(collection_name: str, num_embeddings: int):
    print(f"Checking contents of Chroma collection: {collection_name}")

    chroma_client = chromadb.HttpClient(
        host="localhost",
        port="8000",
        tenant="default_tenant",
        database="default_database",
    )

    collection = chroma_client.get_or_create_collection(name=collection_name)

    number_of_embeddings = collection.count()
    expected_embeddings = num_embeddings
    print(
        f"# of embeddings in collection vs expected: {number_of_embeddings}/{expected_embeddings}"
    )

    assert number_of_embeddings == expected_embeddings, (
        f"Number of rows in generated table ({number_of_embeddings}) "
        f"doesn't match expected value: {expected_embeddings}"
    )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_chroma_destination(
    upload_file: Path,
    chroma_instance,
    tmp_path: Path,
):
    collection_name = "test_collection"
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    stager = ChromaUploadStager(upload_stager_config=ChromaUploadStagerConfig())

    uploader = ChromaUploader(
        connection_config=ChromaConnectionConfig(
            host="localhost",
            port=8000,
            tenant="default_tenant",
            database="default_database",
        ),
        upload_config=ChromaUploaderConfig(collection_name=collection_name),
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
    validate_collection(collection_name=collection_name, num_embeddings=expected_count)


@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "stager", VECTOR_DB_TAG)
def test_chroma_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = ChromaUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
