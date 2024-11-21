import tempfile
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pytest
from psycopg2 import connect

from test.integration.connectors.utils.constants import DESTINATION_TAG, SOURCE_TAG, env_setup_path
from test.integration.connectors.utils.docker_compose import docker_compose_context
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.v2.interfaces import FileData
from unstructured_ingest.v2.processes.connectors.sql.postgres import (
    CONNECTOR_TYPE,
    PostgresAccessConfig,
    PostgresConnectionConfig,
    PostgresDownloader,
    PostgresDownloaderConfig,
    PostgresIndexer,
    PostgresIndexerConfig,
    PostgresUploader,
    PostgresUploadStager,
)

SEED_DATA_ROWS = 20


@contextmanager
def postgres_download_setup() -> None:
    with docker_compose_context(docker_compose_path=env_setup_path / "sql" / "postgres" / "source"):
        connection = connect(
            user="unstructured",
            password="test",
            dbname="test_db",
            host="localhost",
            port=5433,
        )
        with connection.cursor() as cursor:
            for i in range(SEED_DATA_ROWS):
                sql_statment = f"INSERT INTO cars (brand, price) VALUES " f"('brand_{i}', {i})"
                cursor.execute(sql_statment)
            connection.commit()
        yield


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, "sql")
async def test_postgres_source():
    connect_params = {
        "host": "localhost",
        "port": 5433,
        "database": "test_db",
        "user": "unstructured",
        "password": "test",
    }
    with postgres_download_setup():
        with tempfile.TemporaryDirectory() as tmpdir:
            connection_config = PostgresConnectionConfig(
                host=connect_params["host"],
                port=connect_params["port"],
                database=connect_params["database"],
                username=connect_params["user"],
                access_config=PostgresAccessConfig(password=connect_params["password"]),
            )
            indexer = PostgresIndexer(
                connection_config=connection_config,
                index_config=PostgresIndexerConfig(
                    table_name="cars", id_column="car_id", batch_size=5
                ),
            )
            downloader = PostgresDownloader(
                connection_config=connection_config,
                download_config=PostgresDownloaderConfig(
                    fields=["car_id", "brand"], download_dir=Path(tmpdir)
                ),
            )
            await source_connector_validation(
                indexer=indexer,
                downloader=downloader,
                configs=ValidationConfigs(
                    test_id="postgres",
                    expected_num_files=SEED_DATA_ROWS,
                    expected_number_indexed_file_data=4,
                    validate_downloaded_files=True,
                ),
            )


def validate_destination(
    connect_params: dict,
    expected_num_elements: int,
    test_embedding: list[float],
    expected_text: str,
):
    # Run the following validations:
    # * Check that the number of records in the table match the expected value
    # * Given the embedding, make sure it matches the associated text it belongs to
    with connect(**connect_params) as connection:
        cursor = connection.cursor()
        query = "select count(*) from elements;"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        assert (
            count == expected_num_elements
        ), f"dest check failed: got {count}, expected {expected_num_elements}"

        cursor.execute("SELECT embeddings FROM elements order by text limit 1")
        similarity_query = (
            f"SELECT text FROM elements ORDER BY embeddings <-> '{test_embedding}' LIMIT 1;"
        )
        cursor.execute(similarity_query)
        res = cursor.fetchone()
        assert res[0] == expected_text


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "sql")
async def test_postgres_destination(upload_file: Path):
    # the postgres destination connector doesn't leverage the file data but is required as an input,
    # mocking it with arbitrary values to meet the base requirements:
    mock_file_data = FileData(identifier="mock file data", connector_type=CONNECTOR_TYPE)
    with docker_compose_context(
        docker_compose_path=env_setup_path / "sql" / "postgres" / "destination"
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            stager = PostgresUploadStager()
            stager_params = {
                "elements_filepath": upload_file,
                "file_data": mock_file_data,
                "output_dir": Path(tmpdir),
                "output_filename": "test_db",
            }
            if stager.is_async():
                staged_path = await stager.run_async(**stager_params)
            else:
                staged_path = stager.run(**stager_params)

            # The stager should append the `.json` suffix to the output filename passed in.
            assert staged_path.name == "test_db.json"

            connect_params = {
                "host": "localhost",
                "port": 5433,
                "database": "elements",
                "user": "unstructured",
                "password": "test",
            }

            uploader = PostgresUploader(
                connection_config=PostgresConnectionConfig(
                    host=connect_params["host"],
                    port=connect_params["port"],
                    database=connect_params["database"],
                    username=connect_params["user"],
                    access_config=PostgresAccessConfig(password=connect_params["password"]),
                )
            )

            uploader.run(path=staged_path, file_data=mock_file_data)

            staged_df = pd.read_json(staged_path, orient="records", lines=True)
            sample_element = staged_df.iloc[0]
            expected_num_elements = len(staged_df)
            validate_destination(
                connect_params=connect_params,
                expected_num_elements=expected_num_elements,
                expected_text=sample_element["text"],
                test_embedding=sample_element["embeddings"],
            )

            uploader.run(path=staged_path, file_data=mock_file_data)
            validate_destination(
                connect_params=connect_params,
                expected_num_elements=expected_num_elements,
                expected_text=sample_element["text"],
                test_embedding=sample_element["embeddings"],
            )
