import json
import os
from pathlib import Path

import pytest
import snowflake.connector as sf
from _pytest.fixtures import TopRequest

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
    SOURCE_TAG,
    SQL_TAG,
    env_setup_path,
)
from test.integration.connectors.utils.docker import container_context
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
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


def seed_data() -> dict:
    connect_params = {
        "user": "test",
        "password": "test",
        "account": "test",
        "database": "test",
        "host": "snowflake.localhost.localstack.cloud",
    }
    conn = sf.connect(**connect_params)

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
    return connect_params


@pytest.fixture
def source_database_setup() -> dict:
    token = os.getenv("LOCALSTACK_AUTH_TOKEN")
    with container_context(
        image="localstack/snowflake",
        environment={"LOCALSTACK_AUTH_TOKEN": token, "EXTRA_CORS_ALLOWED_ORIGINS": "*"},
        ports={4566: 4566, 443: 443},
        healthcheck_retries=30,
    ):
        connect_params = seed_data()
        yield connect_params


def init_db_destination() -> dict:
    connect_params = {
        "user": "test",
        "password": "test",
        "account": "test",
        "database": "test",
        "host": "snowflake.localhost.localstack.cloud",
    }
    conn = sf.connect(**connect_params)

    file = Path(env_setup_path / "sql" / "snowflake" / "destination" / "snowflake-schema.sql")

    with file.open() as f:
        sql = f.read()

    cur = conn.cursor()
    cur.execute(sql)

    cur.close()
    conn.close()
    return connect_params


@pytest.fixture
def destination_database_setup() -> dict:
    token = os.getenv("LOCALSTACK_AUTH_TOKEN")
    with container_context(
        image="localstack/snowflake",
        environment={"LOCALSTACK_AUTH_TOKEN": token, "EXTRA_CORS_ALLOWED_ORIGINS": "*"},
        ports={4566: 4566, 443: 443},
        healthcheck_retries=30,
    ):
        connect_params = init_db_destination()
        yield connect_params


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, SQL_TAG)
@requires_env("LOCALSTACK_AUTH_TOKEN")
async def test_snowflake_source(temp_dir: Path, source_database_setup: dict):
    connection_config = SnowflakeConnectionConfig(
        access_config=SnowflakeAccessConfig(password="test"),
        account="test",
        user="test",
        database="test",
        host="snowflake.localhost.localstack.cloud",
    )
    indexer = SnowflakeIndexer(
        connection_config=connection_config,
        index_config=SnowflakeIndexerConfig(table_name="cars", id_column="CAR_ID", batch_size=5),
    )
    downloader = SnowflakeDownloader(
        connection_config=connection_config,
        download_config=SnowflakeDownloaderConfig(
            fields=["CAR_ID", "BRAND"], download_dir=temp_dir
        ),
    )
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
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
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env("LOCALSTACK_AUTH_TOKEN")
async def test_snowflake_destination(
    upload_file: Path, temp_dir: Path, destination_database_setup: dict
):
    # the postgres destination connector doesn't leverage the file data but is required as an input,
    # mocking it with arbitrary values to meet the base requirements:
    mock_file_data = FileData(
        identifier="mock file data",
        connector_type=CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(filename=upload_file.name, fullpath=upload_file.name),
    )
    init_db_destination()
    stager = SnowflakeUploadStager()
    staged_path = stager.run(
        elements_filepath=upload_file,
        file_data=mock_file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )

    # The stager should append the `.json` suffix to the output filename passed in.
    assert staged_path.suffix == upload_file.suffix

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
    uploader.precheck()
    uploader.run(path=staged_path, file_data=mock_file_data)

    with staged_path.open("r") as f:
        staged_data = json.load(f)
    expected_num_elements = len(staged_data)
    validate_destination(
        connect_params=connect_params,
        expected_num_elements=expected_num_elements,
    )

    uploader.run(path=staged_path, file_data=mock_file_data)
    validate_destination(
        connect_params=connect_params,
        expected_num_elements=expected_num_elements,
    )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_snowflake_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = SnowflakeUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
