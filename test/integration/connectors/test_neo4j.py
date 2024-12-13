from pathlib import Path

import pytest
from neo4j import AsyncGraphDatabase
from pytest_check import check

from test.integration.connectors.utils.constants import DESTINATION_TAG, env_setup_path
from test.integration.connectors.utils.docker_compose import docker_compose_context
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.neo4j import (
    Label,
    Neo4jAccessConfig,
    Neo4jConnectionConfig,
    Neo4jUploader,
    Neo4jUploaderConfig,
    Neo4jUploadStager,
    Relationship,
)

USERNAME = "neo4j"
PASSWORD = "password"
URI = "neo4j://localhost:7687"

EXPECTED_ELEMENT_COUNT = 42
EXPECTED_CHUNKS_COUNT = 22
EXPECTED_DOCUMENT_COUNT = 1
EXPECTED_NODES_COUNT = 65
DOCKER_COMPOSE_FILEPATH = env_setup_path / "neo4j" / "docker-compose.yml"


@pytest.mark.asyncio
@pytest.mark.tags(DESTINATION_TAG)
async def test_neo4j_destination(upload_file: Path, tmp_path: Path):
    stager = Neo4jUploadStager()
    uploader = Neo4jUploader(
        connection_config=Neo4jConnectionConfig(
            access_config=Neo4jAccessConfig(password=PASSWORD),  # type: ignore
            username=USERNAME,
            uri=URI,
        ),
        upload_config=Neo4jUploaderConfig(database="neo4j"),
    )
    file_data = FileData(
        identifier="mock-file-data",
        connector_type="neo4j",
        source_identifiers=SourceIdentifiers(
            filename=upload_file.name,
            fullpath=upload_file.name,
        ),
    )
    staged_filepath = stager.run(
        upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )

    with docker_compose_context(DOCKER_COMPOSE_FILEPATH):
        await uploader.run_async(staged_filepath, file_data)
        await validate_uploaded_graph()


async def validate_uploaded_graph():
    driver = AsyncGraphDatabase.driver(uri=URI, auth=(USERNAME, PASSWORD))
    try:
        nodes_count = len((await driver.execute_query("MATCH (n) RETURN n"))[0])
        chunk_nodes_count = len(
            (await driver.execute_query(f"MATCH (n: {Label.CHUNK}) RETURN n"))[0]
        )
        document_nodes_count = len(
            (await driver.execute_query(f"MATCH (n: {Label.DOCUMENT}) RETURN n"))[0]
        )
        element_nodes_count = len(
            (await driver.execute_query(f"MATCH (n: {Label.UNSTRUCTURED_ELEMENT}) RETURN n"))[0]
        )
        with check:
            assert nodes_count == EXPECTED_NODES_COUNT

            assert document_nodes_count == EXPECTED_DOCUMENT_COUNT
            assert chunk_nodes_count == EXPECTED_CHUNKS_COUNT
            assert element_nodes_count == EXPECTED_ELEMENT_COUNT

            records, _, _ = await driver.execute_query(
                f"MATCH ()-[r:{Relationship.PART_OF_DOCUMENT}]->(:{Label.DOCUMENT}) RETURN r"
            )
            part_of_document_count = len(records)

            records, _, _ = await driver.execute_query(
                f"MATCH (:{Label.CHUNK})-[r:{Relationship.NEXT_CHUNK}]->(:{Label.CHUNK}) RETURN r"
            )
            next_chunk_count = len(records)

            assert part_of_document_count == EXPECTED_CHUNKS_COUNT + EXPECTED_ELEMENT_COUNT
            assert next_chunk_count == EXPECTED_CHUNKS_COUNT - 1

    finally:
        await driver.close()
