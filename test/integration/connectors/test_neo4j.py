import json
import time
import uuid
from pathlib import Path

import pytest
from neo4j import AsyncGraphDatabase, Driver, GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from pytest_check import check

from test.integration.connectors.utils.constants import DESTINATION_TAG, env_setup_path
from test.integration.connectors.utils.docker_compose import docker_compose_context
from unstructured_ingest.error import DestinationConnectionError
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
DATABASE = "neo4j"

EXPECTED_ELEMENT_COUNT = 42
EXPECTED_CHUNKS_COUNT = 22
EXPECTED_DOCUMENT_COUNT = 1
EXPECTED_NODES_COUNT = 65
DOCKER_COMPOSE_FILEPATH = env_setup_path / "neo4j" / "docker-compose.yml"


# NOTE: Precheck tests are read-only so we utilize the same container for all tests.
# If new tests require clean neo4j container, this fixture's scope should be adjusted.
@pytest.fixture(autouse=True, scope="module")
def _neo4j_server():
    with docker_compose_context(DOCKER_COMPOSE_FILEPATH):
        driver = GraphDatabase.driver(uri=URI, auth=(USERNAME, PASSWORD))
        wait_for_connection(driver)
        driver.close()
        yield


def wait_for_connection(driver: Driver, retries: int = 10, delay_seconds: int = 2):
    attempts = 0
    while attempts < retries:
        try:
            driver.verify_connectivity()
            return
        except ServiceUnavailable:
            time.sleep(delay_seconds)
            attempts += 1

    pytest.fail("Failed to connect with Neo4j server.")


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
        upload_config=Neo4jUploaderConfig(database=DATABASE),
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

    await uploader.run_async(staged_filepath, file_data)
    await validate_uploaded_graph()

    modified_upload_file = tmp_path / f"modified-{upload_file.name}"
    with open(upload_file) as file:
        elements = json.load(file)
        for element in elements:
            element["element_id"] = str(uuid.uuid4())

    with open(modified_upload_file, "w") as file:
        json.dump(elements, file, indent=4)

    staged_filepath = stager.run(
        modified_upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=modified_upload_file.name,
    )
    await uploader.run_async(staged_filepath, file_data)
    await validate_uploaded_graph()


@pytest.mark.tags(DESTINATION_TAG)
class TestPrecheck:
    @pytest.fixture
    def configured_uploader(self) -> Neo4jUploader:
        return Neo4jUploader(
            connection_config=Neo4jConnectionConfig(
                access_config=Neo4jAccessConfig(password=PASSWORD), username=USERNAME, uri=URI
            ),
            upload_config=Neo4jUploaderConfig(database=DATABASE),
        )

    def test_succeeds(self, configured_uploader: Neo4jUploader):
        configured_uploader.precheck()

    def test_fails_on_invalid_password(self, configured_uploader: Neo4jUploader):
        configured_uploader.connection_config.access_config.get_secret_value().password = (
            "invalid-password"
        )
        with pytest.raises(DestinationConnectionError, match="Invalid authentication information."):
            configured_uploader.precheck()

    def test_fails_on_invalid_username(self, configured_uploader: Neo4jUploader):
        configured_uploader.connection_config.username = "invalid-username"
        with pytest.raises(DestinationConnectionError, match="Invalid authentication information."):
            configured_uploader.precheck()

    def test_fails_on_invalid_uri(self, configured_uploader: Neo4jUploader):
        configured_uploader.connection_config.uri = "neo4j://localhst:7687"
        with pytest.raises(
            DestinationConnectionError,
            match="Error in connecting to downstream data source: Cannot resolve address",
        ):
            configured_uploader.precheck()

    def test_fails_on_invalid_database(self, configured_uploader: Neo4jUploader):
        configured_uploader.upload_config.database = "invalid-database"
        with pytest.raises(
            DestinationConnectionError, match="Unable to get a routing table for database"
        ):
            configured_uploader.precheck()


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
        with check:
            assert document_nodes_count == EXPECTED_DOCUMENT_COUNT
        with check:
            assert chunk_nodes_count == EXPECTED_CHUNKS_COUNT
        with check:
            assert element_nodes_count == EXPECTED_ELEMENT_COUNT

        records, _, _ = await driver.execute_query(
            f"MATCH ()-[r:{Relationship.PART_OF_DOCUMENT}]->(:{Label.DOCUMENT}) RETURN r"
        )
        part_of_document_count = len(records)

        records, _, _ = await driver.execute_query(
            f"MATCH (:{Label.CHUNK})-[r:{Relationship.NEXT_CHUNK}]->(:{Label.CHUNK}) RETURN r"
        )
        next_chunk_count = len(records)

        if not check.any_failures():
            with check:
                assert part_of_document_count == EXPECTED_CHUNKS_COUNT + EXPECTED_ELEMENT_COUNT
            with check:
                assert next_chunk_count == EXPECTED_CHUNKS_COUNT - 1

    finally:
        await driver.close()
