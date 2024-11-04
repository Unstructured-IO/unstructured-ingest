import json
import uuid
from pathlib import Path

import pytest
from qdrant_client import AsyncQdrantClient

from test.integration.connectors.utils.constants import DESTINATION_TAG, env_setup_path
from test.integration.connectors.utils.docker_compose import docker_compose_context
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.qdrant import (
    CONNECTOR_TYPE,
    QdrantAccessConfig,
    QdrantConnectionConfig,
    QdrantUploader,
    QdrantUploaderConfig,
    QdrantUploadStager,
    QdrantUploadStagerConfig,
)

LOCATION = "http://localhost:6333"
COLLECTION_NAME = uuid.uuid4().hex
DOCKER_COMPOSE_DIR = env_setup_path / "qdrant"
VECTOR_SIZE = 384
VECTOR_DISTANCE = "Cosine"


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
async def test_qdrant_destination(upload_file: Path, tmp_path: Path):
    access_config = QdrantAccessConfig()
    connection_config = QdrantConnectionConfig(
        access_config=access_config,
        collection_name=COLLECTION_NAME,
        location=LOCATION,
    )
    stager = QdrantUploadStager(
        upload_stager_config=QdrantUploadStagerConfig(),
    )
    uploader = QdrantUploader(
        connection_config=connection_config,
        upload_config=QdrantUploaderConfig(),
    )

    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock-file-data",
    )
    with open(upload_file) as file:
        EXPECTED_POINT_COUNT = len(json.load(file))

    staged_upload_file = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )

    with docker_compose_context(DOCKER_COMPOSE_DIR):
        async_client = AsyncQdrantClient(location=LOCATION)
        await async_client.create_collection(
            COLLECTION_NAME,
            vectors_config={
                "size": VECTOR_SIZE,
                "distance": VECTOR_DISTANCE,
            },
        )

        await uploader.run_async(path=staged_upload_file, file_data=file_data)

        collection = await async_client.get_collection(COLLECTION_NAME)

    assert collection.points_count == EXPECTED_POINT_COUNT
