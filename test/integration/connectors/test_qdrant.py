import json
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

import pytest
from _pytest.fixtures import TopRequest
from qdrant_client import AsyncQdrantClient

from test.integration.connectors.utils.constants import DESTINATION_TAG, VECTOR_DB_TAG
from test.integration.connectors.utils.docker import container_context
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.qdrant.cloud import (
    CloudQdrantAccessConfig,
    CloudQdrantConnectionConfig,
    CloudQdrantUploader,
    CloudQdrantUploaderConfig,
    CloudQdrantUploadStager,
    CloudQdrantUploadStagerConfig,
)
from unstructured_ingest.v2.processes.connectors.qdrant.local import (
    CONNECTOR_TYPE as LOCAL_CONNECTOR_TYPE,
)
from unstructured_ingest.v2.processes.connectors.qdrant.local import (
    LocalQdrantConnectionConfig,
    LocalQdrantUploader,
    LocalQdrantUploaderConfig,
    LocalQdrantUploadStager,
    LocalQdrantUploadStagerConfig,
)
from unstructured_ingest.v2.processes.connectors.qdrant.server import (
    CONNECTOR_TYPE as SERVER_CONNECTOR_TYPE,
)
from unstructured_ingest.v2.processes.connectors.qdrant.server import (
    ServerQdrantConnectionConfig,
    ServerQdrantUploader,
    ServerQdrantUploaderConfig,
    ServerQdrantUploadStager,
    ServerQdrantUploadStagerConfig,
)

COLLECTION_NAME = f"test-coll-{uuid.uuid4().hex[:12]}"
VECTORS_CONFIG = {"size": 384, "distance": "Cosine"}


@asynccontextmanager
async def qdrant_client(client_params: dict) -> AsyncGenerator[AsyncQdrantClient, None]:
    client = AsyncQdrantClient(**client_params)
    try:
        yield client
    finally:
        await client.close()


async def validate_upload(client: AsyncQdrantClient, upload_file: Path):
    with upload_file.open() as upload_fp:
        elements = json.load(upload_fp)
    expected_point_count = len(elements)
    first_element = elements[0]
    expected_text = first_element["text"]
    embeddings = first_element["embeddings"]
    collection = await client.get_collection(COLLECTION_NAME)
    assert collection.points_count == expected_point_count

    response = await client.query_points(COLLECTION_NAME, query=embeddings, limit=1)
    assert response.points[0].payload is not None
    assert response.points[0].payload["text"] == expected_text


@pytest.mark.asyncio
@pytest.mark.tags(LOCAL_CONNECTOR_TYPE, DESTINATION_TAG, "qdrant", VECTOR_DB_TAG)
async def test_qdrant_destination_local(upload_file: Path, tmp_path: Path):
    connection_kwargs = {"path": str(tmp_path / "qdrant")}
    async with qdrant_client(connection_kwargs) as client:
        await client.create_collection(COLLECTION_NAME, vectors_config=VECTORS_CONFIG)
    AsyncQdrantClient(**connection_kwargs)
    stager = LocalQdrantUploadStager(
        upload_stager_config=LocalQdrantUploadStagerConfig(),
    )
    uploader = LocalQdrantUploader(
        connection_config=LocalQdrantConnectionConfig(**connection_kwargs),
        upload_config=LocalQdrantUploaderConfig(collection_name=COLLECTION_NAME),
    )

    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=LOCAL_CONNECTOR_TYPE,
        identifier="mock-file-data",
    )

    staged_upload_file = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )

    if uploader.is_async():
        await uploader.run_async(path=staged_upload_file, file_data=file_data)
    else:
        uploader.run(path=upload_file, file_data=file_data)
    async with qdrant_client(connection_kwargs) as client:
        await validate_upload(client=client, upload_file=upload_file)


@pytest.fixture
def docker_context():
    with container_context(image="qdrant/qdrant:latest", ports={"6333": "6333"}) as container:
        yield container


@pytest.mark.asyncio
@pytest.mark.tags(SERVER_CONNECTOR_TYPE, DESTINATION_TAG, "qdrant", VECTOR_DB_TAG)
async def test_qdrant_destination_server(upload_file: Path, tmp_path: Path, docker_context):
    connection_kwargs = {"location": "http://localhost:6333"}
    async with qdrant_client(connection_kwargs) as client:
        await client.create_collection(COLLECTION_NAME, vectors_config=VECTORS_CONFIG)
    AsyncQdrantClient(**connection_kwargs)
    stager = ServerQdrantUploadStager(
        upload_stager_config=ServerQdrantUploadStagerConfig(),
    )
    uploader = ServerQdrantUploader(
        connection_config=ServerQdrantConnectionConfig(**connection_kwargs),
        upload_config=ServerQdrantUploaderConfig(collection_name=COLLECTION_NAME),
    )

    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=SERVER_CONNECTOR_TYPE,
        identifier="mock-file-data",
    )

    staged_upload_file = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )
    uploader.precheck()
    if uploader.is_async():
        await uploader.run_async(path=staged_upload_file, file_data=file_data)
    else:
        uploader.run(path=upload_file, file_data=file_data)
    async with qdrant_client(connection_kwargs) as client:
        await validate_upload(client=client, upload_file=upload_file)


@pytest.mark.asyncio
@pytest.mark.tags(SERVER_CONNECTOR_TYPE, DESTINATION_TAG, "qdrant", VECTOR_DB_TAG)
@requires_env("QDRANT_API_KEY", "QDRANT_SERVER_URL")
async def test_qdrant_destination_cloud(upload_file: Path, tmp_path: Path):
    server_url = os.environ["QDRANT_SERVER_URL"]
    api_key = os.environ["QDRANT_API_KEY"]
    connection_kwargs = {"location": server_url, "api_key": api_key}
    async with qdrant_client(connection_kwargs) as client:
        await client.create_collection(COLLECTION_NAME, vectors_config=VECTORS_CONFIG)
    AsyncQdrantClient(**connection_kwargs)

    stager = CloudQdrantUploadStager(
        upload_stager_config=CloudQdrantUploadStagerConfig(),
    )
    uploader = CloudQdrantUploader(
        connection_config=CloudQdrantConnectionConfig(
            url=server_url,
            access_config=CloudQdrantAccessConfig(
                api_key=api_key,
            ),
        ),
        upload_config=CloudQdrantUploaderConfig(collection_name=COLLECTION_NAME),
    )

    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=SERVER_CONNECTOR_TYPE,
        identifier="mock-file-data",
    )

    staged_upload_file = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )
    uploader.precheck()
    if uploader.is_async():
        await uploader.run_async(path=staged_upload_file, file_data=file_data)
    else:
        uploader.run(path=staged_upload_file, file_data=file_data)
    async with qdrant_client(connection_kwargs) as client:
        await validate_upload(client=client, upload_file=upload_file)


@pytest.mark.tags(SERVER_CONNECTOR_TYPE, DESTINATION_TAG, "qdrant", VECTOR_DB_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_qdrant_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = LocalQdrantUploadStager(
        upload_stager_config=LocalQdrantUploadStagerConfig(),
    )
    stager_validation(
        configs=StagerValidationConfigs(test_id=LOCAL_CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
