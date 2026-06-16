import json
import time
from pathlib import Path

import pytest
import requests
import weaviate
from weaviate.classes.config import DataType
from weaviate.client import WeaviateClient
from weaviate.collections.classes.config import PropertyConfig

from test.integration.connectors.utils.constants import DESTINATION_TAG, VECTOR_DB_TAG
from test.integration.connectors.utils.docker import container_context
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.weaviate.local import (
    CONNECTOR_TYPE,
    LocalWeaviateConnectionConfig,
    LocalWeaviateUploader,
    LocalWeaviateUploaderConfig,
    LocalWeaviateUploadStager,
)

COLLECTION_NAME = "elements"


def wait_for_container(timeout: int = 10, interval: int = 1) -> None:
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            requests.get("http://localhost:8080/v1/.well-known/read", timeout=1)
            return
        except Exception as e:
            print(f"Failed to validate container healthy, sleeping for {interval} seconds: {e}")
            time.sleep(interval)
    raise TimeoutError("Docker container never came up healthy")


@pytest.fixture
def weaviate_instance():
    with container_context(
        image="semitechnologies/weaviate:1.27.3",
        ports={8080: 8080, 50051: 50051},
    ) as ctx:
        wait_for_container()
        yield ctx


@pytest.fixture
def collection(weaviate_instance, collections_schema_config: dict) -> str:
    with weaviate.connect_to_local() as weaviate_client:
        weaviate_client.collections.create_from_dict(config=collections_schema_config)
    return COLLECTION_NAME


def find_property(properties: list[PropertyConfig], name: str) -> PropertyConfig:
    matches = [prop for prop in properties if prop.name == name]
    assert matches, f"property '{name}' not found among {[p.name for p in properties]}"
    return matches[0]


def assert_data_source_version_is_text(client: WeaviateClient, collection_name: str) -> None:
    """The created collection must declare metadata.data_source.version as `text`.

    Without an explicit declaration Weaviate auto-schema infers the type from the
    first inserted value, which locks the property to `uuid` when an ETag happens to
    be UUID-shaped and then rejects non-UUID version strings.
    """
    config = client.collections.get(collection_name).config.get()
    metadata = find_property(config.properties, "metadata")
    data_source = find_property(metadata.nested_properties or [], "data_source")
    version = find_property(data_source.nested_properties or [], "version")
    assert version.data_type == DataType.TEXT, (
        f"expected metadata.data_source.version to be {DataType.TEXT}, got {version.data_type}"
    )


def get_count(client: WeaviateClient) -> int:
    collection = client.collections.get(COLLECTION_NAME)
    resp = collection.aggregate.over_all(total_count=True)
    return resp.total_count


def validate_count(expected_count: int, retries: int = 10, interval: int = 1) -> None:
    with weaviate.connect_to_local() as weaviate_client:
        current_count = get_count(client=weaviate_client)
        retry_count = 0
        while current_count != expected_count and retry_count < retries:
            retry_count += 1
            time.sleep(interval)
            current_count = get_count(client=weaviate_client)
        assert current_count == expected_count, (
            f"Expected count ({expected_count}) doesn't match how "
            f"much came back from collection: {current_count}"
        )


def run_uploader_and_validate(
    uploader: LocalWeaviateUploader, path: Path, file_data: FileData, expected_count: int
):
    uploader.precheck()
    uploader.run(path=path, file_data=file_data)
    validate_count(expected_count=expected_count)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_weaviate_local_destination(upload_file: Path, collection: str, tmp_path: Path):
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    stager = LocalWeaviateUploadStager()

    staged_filepath = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )
    dynamic_uploader = LocalWeaviateUploader(
        upload_config=LocalWeaviateUploaderConfig(
            collection=COLLECTION_NAME,
        ),
        connection_config=LocalWeaviateConnectionConfig(),
    )
    fixed_size_uploader = LocalWeaviateUploader(
        upload_config=LocalWeaviateUploaderConfig(
            collection=COLLECTION_NAME, batch_size=10, dynamic_batch=False
        ),
        connection_config=LocalWeaviateConnectionConfig(),
    )
    rate_limited_uploader = LocalWeaviateUploader(
        upload_config=LocalWeaviateUploaderConfig(
            collection=COLLECTION_NAME, requests_per_minute=50, dynamic_batch=False
        ),
        connection_config=LocalWeaviateConnectionConfig(),
    )
    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    expected_count = len(staged_elements)

    run_uploader_and_validate(
        uploader=dynamic_uploader,
        path=staged_filepath,
        file_data=file_data,
        expected_count=expected_count,
    )
    run_uploader_and_validate(
        uploader=fixed_size_uploader,
        path=staged_filepath,
        file_data=file_data,
        expected_count=expected_count,
    )
    run_uploader_and_validate(
        uploader=rate_limited_uploader,
        path=staged_filepath,
        file_data=file_data,
        expected_count=expected_count,
    )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
def test_weaviate_local_create_destination(weaviate_instance):
    uploader = LocalWeaviateUploader(
        upload_config=LocalWeaviateUploaderConfig(),
        connection_config=LocalWeaviateConnectionConfig(),
    )
    collection_name = "system_CREATED-123"
    formatted_collection_name = "System_CREATED_123"
    created = uploader.create_destination(destination_name=collection_name)
    assert created
    with uploader.connection_config.get_client() as weaviate_client:
        assert weaviate_client.collections.exists(name=formatted_collection_name)
        assert_data_source_version_is_text(weaviate_client, formatted_collection_name)

    created = uploader.create_destination(destination_name=collection_name)
    assert not created
