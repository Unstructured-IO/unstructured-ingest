import os
import tempfile
from pathlib import Path

import docker
import faker
import pytest
import snowflake.connector as sf

from test.integration.connectors.utils.constants import SOURCE_TAG, env_setup_path
from test.integration.connectors.utils.docker import container_context
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.connectors.sql.snowflake import (
    CONNECTOR_TYPE,
    SnowflakeAccessConfig,
    SnowflakeConnectionConfig,
    SnowflakeDownloader,
    SnowflakeDownloaderConfig,
    SnowflakeIndexer,
    SnowflakeIndexerConfig,
)

faker = faker.Faker()

SEED_DATA_ROWS = 40


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
    for _ in range(SEED_DATA_ROWS):
        sql_statment = (
            f"INSERT INTO cars (brand, price) VALUES " f"('{faker.word()}', {faker.random_int()})"
        )
        cur.execute(sql_statment)

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
        healthcheck_timeout=30,
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
                    expected_num_files=40,
                ),
            )
