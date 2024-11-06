import json
import uuid
from pathlib import Path
from typing import Literal

import pytest
from qdrant_client import AsyncQdrantClient

from test.integration.connectors.utils.constants import DESTINATION_TAG, env_setup_path
from test.integration.connectors.utils.docker_compose import docker_compose_context
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.qdrant import (
    CONNECTOR_TYPE,
    QdrantConnectionConfig,
    QdrantUploader,
    QdrantUploaderConfig,
    QdrantUploadStager,
    QdrantUploadStagerConfig,
)

SERVER_URL = "http://localhost:6333"
LOCAL_DIRECTORY_NAME = "qdrant"
COLLECTION_NAME = uuid.uuid4().hex
DOCKER_COMPOSE_DIR = env_setup_path / "qdrant"
VECTORS_CONFIG = {"size": 384, "distance": "Cosine"}


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
@pytest.mark.parametrize("mode", ["remote", "local"])
async def test_qdrant_destination(
    upload_file: Path, tmp_path: Path, mode: Literal["remote", "local"]
):
    connection_kwargs = (
        {"location": SERVER_URL}
        if mode == "remote"
        else {"path": str(tmp_path / LOCAL_DIRECTORY_NAME)}
    )

    with open(upload_file) as file:
        elements = json.load(file)
        EXPECTED_POINT_COUNT = len(elements)
        EXPECTED_TEXT = elements[0]["text"]
        EMBEDDINGS = elements[0]["embeddings"]

    with docker_compose_context(DOCKER_COMPOSE_DIR):
        async_client = AsyncQdrantClient(**connection_kwargs)
        await async_client.create_collection(COLLECTION_NAME, vectors_config=VECTORS_CONFIG)
        await async_client.close()

        stager = QdrantUploadStager(
            upload_stager_config=QdrantUploadStagerConfig(),
        )
        uploader = QdrantUploader(
            connection_config=QdrantConnectionConfig(**connection_kwargs),
            upload_config=QdrantUploaderConfig(collection_name=COLLECTION_NAME),
        )

        file_data = FileData(
            source_identifiers=SourceIdentifiers(
                fullpath=upload_file.name, filename=upload_file.name
            ),
            connector_type=CONNECTOR_TYPE,
            identifier="mock-file-data",
        )

        staged_upload_file = stager.run(
            elements_filepath=upload_file,
            file_data=file_data,
            output_dir=tmp_path,
            output_filename=upload_file.name,
        )

        await uploader.run_async(path=staged_upload_file, file_data=file_data)

        async_client = AsyncQdrantClient(**connection_kwargs)
        collection = await async_client.get_collection(COLLECTION_NAME)
        assert collection.points_count == EXPECTED_POINT_COUNT

        response = await async_client.query_points(COLLECTION_NAME, query=EMBEDDINGS, limit=1)
        assert response.points[0].payload is not None
        assert response.points[0].payload["text"] == EXPECTED_TEXT
