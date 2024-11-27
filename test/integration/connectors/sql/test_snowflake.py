import os
import tempfile
from pathlib import Path

import docker
import pandas as pd
import pytest
import snowflake.connector as sf

from test.integration.connectors.utils.constants import DESTINATION_TAG, SOURCE_TAG, env_setup_path
from test.integration.connectors.utils.docker import container_context
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces import FileData
from unstructured_ingest.v2.processes.connectors.sql.snowflake import (
    CONNECTOR_TYPE,
    SnowflakeAccessConfig,
    SnowflakeConnectionConfig,
    SnowflakeDownloader,
    SnowflakeDownloaderConfig,
    SnowflakeIndexer,
    SnowflakeIndexerConfig,
    SnowflakeUploader,
    SnowflakeUploadStager,
)

SEED_DATA_ROWS = 20


def seed_data():
    conn = sf.connect(
        user="test",
        password="test",
        account="test",
        database="test",
        host="snowflake.localhost.localstack.cloud",
    )

    file = Path(env_setup_path / "sql" / "snowflake" / "source" / "snowflake-schema.sql")

    with file.open() as f:
        sql = f.read()

    cur = conn.cursor()
    cur.execute(sql)
    for i in range(SEED_DATA_ROWS):
        sql_statment = f"INSERT INTO cars (brand, price) VALUES " f"('brand_{i}', {i})"
        cur.execute(sql_statment)

    cur.close()
    conn.close()


def init_db_destination():
    conn = sf.connect(
        user="test",
        password="test",
        account="test",
        database="test",
        host="snowflake.localhost.localstack.cloud",
    )

    file = Path(env_setup_path / "sql" / "snowflake" / "destination" / "snowflake-schema.sql")

    with file.open() as f:
        sql = f.read()

    cur = conn.cursor()
    cur.execute(sql)

    cur.close()
    conn.close()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, "sql")
@requires_env("LOCALSTACK_AUTH_TOKEN")
async def test_snowflake_source():
    docker_client = docker.from_env()
    token = os.getenv("LOCALSTACK_AUTH_TOKEN")
    with container_context(
        docker_client=docker_client,
        image="localstack/snowflake",
        environment={"LOCALSTACK_AUTH_TOKEN": token, "EXTRA_CORS_ALLOWED_ORIGINS": "*"},
        ports={4566: 4566, 443: 443},
        healthcheck_retries=30,
    ):
        seed_data()
        with tempfile.TemporaryDirectory() as tmpdir:
            connection_config = SnowflakeConnectionConfig(
                access_config=SnowflakeAccessConfig(password="test"),
                account="test",
                user="test",
                database="test",
                host="snowflake.localhost.localstack.cloud",
            )
            indexer = SnowflakeIndexer(
                connection_config=connection_config,
                index_config=SnowflakeIndexerConfig(
                    table_name="cars", id_column="CAR_ID", batch_size=5
                ),
            )
            downloader = SnowflakeDownloader(
                connection_config=connection_config,
                download_config=SnowflakeDownloaderConfig(
                    fields=["CAR_ID", "BRAND"], download_dir=Path(tmpdir)
                ),
            )
            await source_connector_validation(
                indexer=indexer,
                downloader=downloader,
                configs=ValidationConfigs(
                    test_id="snowflake",
                    expected_num_files=SEED_DATA_ROWS,
                    expected_number_indexed_file_data=4,
                    validate_downloaded_files=True,
                ),
            )


def validate_destination(
    connect_params: dict,
    expected_num_elements: int,
):
    # Run the following validations:
    # * Check that the number of records in the table match the expected value
    # * Given the embedding, make sure it matches the associated text it belongs to
    conn = sf.connect(**connect_params)
    cursor = conn.cursor()
    try:
        query = "select count(*) from elements;"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        assert (
            count == expected_num_elements
        ), f"dest check failed: got {count}, expected {expected_num_elements}"
    finally:
        cursor.close()
        conn.close()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "sql")
@requires_env("LOCALSTACK_AUTH_TOKEN")
async def test_snowflake_destination(upload_file: Path):
    # the postgres destination connector doesn't leverage the file data but is required as an input,
    # mocking it with arbitrary values to meet the base requirements:
    mock_file_data = FileData(identifier="mock file data", connector_type=CONNECTOR_TYPE)
    docker_client = docker.from_env()
    token = os.getenv("LOCALSTACK_AUTH_TOKEN")
    with container_context(
        docker_client=docker_client,
        image="localstack/snowflake",
        environment={"LOCALSTACK_AUTH_TOKEN": token, "EXTRA_CORS_ALLOWED_ORIGINS": "*"},
        ports={4566: 4566, 443: 443},
        healthcheck_retries=30,
    ):
        init_db_destination()
        with tempfile.TemporaryDirectory() as tmpdir:
            stager = SnowflakeUploadStager()
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
                "user": "test",
                "password": "test",
                "account": "test",
                "database": "test",
                "host": "snowflake.localhost.localstack.cloud",
            }

            uploader = SnowflakeUploader(
                connection_config=SnowflakeConnectionConfig(
                    access_config=SnowflakeAccessConfig(password=connect_params["password"]),
                    account=connect_params["account"],
                    user=connect_params["user"],
                    database=connect_params["database"],
                    host=connect_params["host"],
                )
            )

            uploader.run(path=staged_path, file_data=mock_file_data)

            staged_df = pd.read_json(staged_path, orient="records", lines=True)
            expected_num_elements = len(staged_df)
            validate_destination(
                connect_params=connect_params,
                expected_num_elements=expected_num_elements,
            )

            uploader.run(path=staged_path, file_data=mock_file_data)
            validate_destination(
                connect_params=connect_params,
                expected_num_elements=expected_num_elements,
            )
