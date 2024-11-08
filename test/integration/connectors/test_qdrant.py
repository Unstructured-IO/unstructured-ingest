import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

import pytest
from qdrant_client import AsyncQdrantClient

from test.integration.connectors.utils.constants import DESTINATION_TAG, env_setup_path
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
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

SERVER_URL = "http://localhost:6333"
COLLECTION_NAME = f"test-coll-{uuid.uuid4().hex[:12]}"
DOCKER_COMPOSE_DIR = env_setup_path / "qdrant"
VECTORS_CONFIG = {"size": 384, "distance": "Cosine"}


# @pytest.mark.asyncio
# @pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
# async def test_qdrant_destination_server(
#     upload_file: Path, tmp_path: Path
# ):
#     connection_kwargs = (
#         {"location": SERVER_URL}
#         if mode == "remote"
#         else {"path": str(tmp_path / LOCAL_DIRECTORY_NAME)}
#     )
#
#     with open(upload_file) as file:
#         elements = json.load(file)
#         EXPECTED_POINT_COUNT = len(elements)
#         EXPECTED_TEXT = elements[0]["text"]
#         EMBEDDINGS = elements[0]["embeddings"]
#
#     with docker_compose_context(DOCKER_COMPOSE_DIR):
#         async_client = AsyncQdrantClient(**connection_kwargs)
#         await async_client.create_collection(COLLECTION_NAME, vectors_config=VECTORS_CONFIG)
#         await async_client.close()
#
#         stager = QdrantUploadStager(
#             upload_stager_config=QdrantUploadStagerConfig(),
#         )
#         uploader = QdrantUploader(
#             connection_config=QdrantConnectionConfig(**connection_kwargs),
#             upload_config=QdrantUploaderConfig(collection_name=COLLECTION_NAME),
#         )
#
#         file_data = FileData(
#             source_identifiers=SourceIdentifiers(
#                 fullpath=upload_file.name, filename=upload_file.name
#             ),
#             connector_type=CONNECTOR_TYPE,
#             identifier="mock-file-data",
#         )
#
#         staged_upload_file = stager.run(
#             elements_filepath=upload_file,
#             file_data=file_data,
#             output_dir=tmp_path,
#             output_filename=upload_file.name,
#         )
#
#         await uploader.run_async(path=staged_upload_file, file_data=file_data)
#
#         async_client = AsyncQdrantClient(**connection_kwargs)
#         collection = await async_client.get_collection(COLLECTION_NAME)
#         assert collection.points_count == EXPECTED_POINT_COUNT
#
#         response = await async_client.query_points(COLLECTION_NAME, query=EMBEDDINGS, limit=1)
#         assert response.points[0].payload is not None
#         assert response.points[0].payload["text"] == EXPECTED_TEXT


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
@pytest.mark.tags(LOCAL_CONNECTOR_TYPE, DESTINATION_TAG, "qdrant")
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
