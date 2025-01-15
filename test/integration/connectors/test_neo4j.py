import json
import time
import uuid
from datetime import datetime
from pathlib import Path

import pytest
from neo4j import AsyncGraphDatabase, Driver, GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from pytest_check import check

from test.integration.connectors.utils.constants import DESTINATION_TAG, GRAPH_DB_TAG
from test.integration.connectors.utils.docker import container_context
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.chunking import elements_from_base64_gzipped_json
from unstructured_ingest.v2.interfaces.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.v2.processes.connectors.neo4j import (
    CONNECTOR_TYPE,
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

EXPECTED_DOCUMENT_COUNT = 1


# NOTE: Precheck tests are read-only so we utilize the same container for all tests.
# If new tests require clean neo4j container, this fixture's scope should be adjusted.
@pytest.fixture(autouse=True, scope="module")
def _neo4j_server():
    with container_context(
        image="neo4j:latest", environment={"NEO4J_AUTH": "neo4j/password"}, ports={"7687": "7687"}
    ):
        driver = GraphDatabase.driver(uri=URI, auth=(USERNAME, PASSWORD))
        wait_for_connection(driver)
        driver.close()
        yield


@pytest.mark.asyncio
@pytest.mark.tags(DESTINATION_TAG, CONNECTOR_TYPE, GRAPH_DB_TAG)
async def test_neo4j_destination(upload_file: Path, tmp_path: Path):
    stager = Neo4jUploadStager()
    uploader = Neo4jUploader(
        connection_config=Neo4jConnectionConfig(
            access_config=Neo4jAccessConfig(password=PASSWORD),  # type: ignore
            username=USERNAME,
            uri=URI,
            database=DATABASE,
        ),
        upload_config=Neo4jUploaderConfig(),
    )
    file_data = FileData(
        identifier="mock-file-data",
        connector_type="neo4j",
        source_identifiers=SourceIdentifiers(
            filename=upload_file.name,
            fullpath=upload_file.name,
        ),
        metadata=FileDataSourceMetadata(
            date_created=str(datetime(2022, 1, 1).timestamp()),
            date_modified=str(datetime(2022, 1, 2).timestamp()),
        ),
    )
    staged_filepath = stager.run(
        upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )

    await uploader.run_async(staged_filepath, file_data)
    await validate_uploaded_graph(upload_file)

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
    await validate_uploaded_graph(modified_upload_file)


@pytest.mark.tags(DESTINATION_TAG, CONNECTOR_TYPE, GRAPH_DB_TAG)
class TestPrecheck:
    @pytest.fixture
    def configured_uploader(self) -> Neo4jUploader:
        return Neo4jUploader(
            connection_config=Neo4jConnectionConfig(
                access_config=Neo4jAccessConfig(password=PASSWORD),  # type: ignore
                username=USERNAME,
                uri=URI,
                database=DATABASE,
            ),
            upload_config=Neo4jUploaderConfig(),
        )

    def test_succeeds(self, configured_uploader: Neo4jUploader):
        configured_uploader.precheck()

    def test_fails_on_invalid_password(self, configured_uploader: Neo4jUploader):
        configured_uploader.connection_config.access_config.get_secret_value().password = (
            "invalid-password"
        )
        with pytest.raises(
            DestinationConnectionError,
            match="{code: Neo.ClientError.Security.Unauthorized}",
        ):
            configured_uploader.precheck()

    def test_fails_on_invalid_username(self, configured_uploader: Neo4jUploader):
        configured_uploader.connection_config.username = "invalid-username"
        with pytest.raises(
            DestinationConnectionError, match="{code: Neo.ClientError.Security.Unauthorized}"
        ):
            configured_uploader.precheck()

    @pytest.mark.parametrize(
        ("uri", "expected_error_msg"),
        [
            ("neo4j://localhst:7687", "Cannot resolve address"),
            ("neo4j://localhost:7777", "Unable to retrieve routing information"),
        ],
    )
    def test_fails_on_invalid_uri(
        self, configured_uploader: Neo4jUploader, uri: str, expected_error_msg: str
    ):
        configured_uploader.connection_config.uri = uri
        with pytest.raises(DestinationConnectionError, match=expected_error_msg):
            configured_uploader.precheck()

    def test_fails_on_invalid_database(self, configured_uploader: Neo4jUploader):
        configured_uploader.connection_config.database = "invalid-database"
        with pytest.raises(
            DestinationConnectionError, match="{code: Neo.ClientError.Database.DatabaseNotFound}"
        ):
            configured_uploader.precheck()


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


async def validate_uploaded_graph(upload_file: Path):
    with open(upload_file) as file:
        elements = json.load(file)

    for element in elements:
        if "orig_elements" in element["metadata"]:
            element["metadata"]["orig_elements"] = elements_from_base64_gzipped_json(
                element["metadata"]["orig_elements"]
            )
        else:
            element["metadata"]["orig_elements"] = []

    expected_chunks_count = len(elements)
    expected_element_count = len(
        {
            origin_element["element_id"]
            for chunk in elements
            for origin_element in chunk["metadata"]["orig_elements"]
        }
    )
    expected_nodes_count = expected_chunks_count + expected_element_count + EXPECTED_DOCUMENT_COUNT

    driver = AsyncGraphDatabase.driver(uri=URI, auth=(USERNAME, PASSWORD))
    try:
        nodes_count = len((await driver.execute_query("MATCH (n) RETURN n"))[0])
        chunk_nodes_count = len(
            (await driver.execute_query(f"MATCH (n: {Label.CHUNK.value}) RETURN n"))[0]
        )
        document_nodes_count = len(
            (await driver.execute_query(f"MATCH (n: {Label.DOCUMENT.value}) RETURN n"))[0]
        )
        element_nodes_count = len(
            (await driver.execute_query(f"MATCH (n: {Label.UNSTRUCTURED_ELEMENT.value}) RETURN n"))[
                0
            ]
        )
        with check:
            assert nodes_count == expected_nodes_count
        with check:
            assert document_nodes_count == EXPECTED_DOCUMENT_COUNT
        with check:
            assert chunk_nodes_count == expected_chunks_count
        with check:
            assert element_nodes_count == expected_element_count

        records, _, _ = await driver.execute_query(
            f"""
            MATCH ()-[r:{Relationship.PART_OF_DOCUMENT.value}]->(:{Label.DOCUMENT.value})
            RETURN r
            """
        )
        part_of_document_count = len(records)

        records, _, _ = await driver.execute_query(
            f"""
            MATCH (:{Label.CHUNK.value})-[r:{Relationship.NEXT_CHUNK.value}]->(:{Label.CHUNK.value})
            RETURN r
            """
        )
        next_chunk_count = len(records)

        if not check.any_failures():
            with check:
                assert part_of_document_count == expected_chunks_count + expected_element_count
            with check:
                assert next_chunk_count == expected_chunks_count - 1

    finally:
        await driver.close()
