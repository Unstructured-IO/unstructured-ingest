import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from databricks.sql import connect
from databricks.sql.client import Connection as DeltaTableConnection
from databricks.sql.client import Cursor as DeltaTableCursor
from pydantic import BaseModel, SecretStr

from test.integration.connectors.utils.constants import DESTINATION_TAG, SQL_TAG, env_setup_path
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connectors.sql.databricks_delta_tables import (
    CONNECTOR_TYPE,
    DatabricksDeltaTablesAccessConfig,
    DatabricksDeltaTablesConnectionConfig,
    DatabricksDeltaTablesUploader,
    DatabricksDeltaTablesUploaderConfig,
    DatabricksDeltaTablesUploadStager,
)

CATALOG = "utic-dev-tech-fixtures"


class EnvData(BaseModel):
    server_hostname: str
    http_path: str
    access_token: SecretStr


def get_env_data() -> EnvData:
    return EnvData(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_ACCESS_TOKEN"],
    )


def get_destination_schema(new_table_name: str) -> str:
    p = Path(env_setup_path / "sql" / "databricks_delta_tables" / "destination" / "schema.sql")
    with p.open() as f:
        data_lines = f.readlines()
    data_lines[0] = data_lines[0].replace("elements", new_table_name)
    data = "".join([line.strip() for line in data_lines])
    return data


@contextmanager
def get_connection() -> DeltaTableConnection:
    env_data = get_env_data()
    with connect(
        server_hostname=env_data.server_hostname,
        http_path=env_data.http_path,
        access_token=env_data.access_token.get_secret_value(),
    ) as connection:
        yield connection


@contextmanager
def get_cursor() -> DeltaTableCursor:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"USE CATALOG '{CATALOG}'")
            yield cursor


@pytest.fixture
def destination_table() -> str:
    random_id = str(uuid4())[:8]
    table_name = f"elements_{random_id}"
    destination_schema = get_destination_schema(new_table_name=table_name)
    with get_cursor() as cursor:
        logger.info(f"creating table: {table_name}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(destination_schema)

    yield table_name
    with get_cursor() as cursor:
        logger.info(f"dropping table: {table_name}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


def validate_destination(expected_num_elements: int, table_name: str, retries=30, interval=1):
    with get_cursor() as cursor:
        for i in range(retries):
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            if count == expected_num_elements:
                break
            logger.info(f"retry attempt {i}: expected {expected_num_elements} != count {count}")
            time.sleep(interval)
        assert (
            count == expected_num_elements
        ), f"dest check failed: got {count}, expected {expected_num_elements}"


@pytest.mark.asyncio
@pytest.mark.skip("Resources take too long to spin up to run in CI")
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_ACCESS_TOKEN")
async def test_databricks_delta_tables_destination(
    upload_file: Path, temp_dir: Path, destination_table: str
):
    env_data = get_env_data()
    mock_file_data = FileData(
        identifier="mock file data",
        connector_type=CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(filename=upload_file.name, fullpath=upload_file.name),
    )
    stager = DatabricksDeltaTablesUploadStager()
    staged_path = stager.run(
        elements_filepath=upload_file,
        file_data=mock_file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )

    assert staged_path.suffix == upload_file.suffix

    uploader = DatabricksDeltaTablesUploader(
        connection_config=DatabricksDeltaTablesConnectionConfig(
            access_config=DatabricksDeltaTablesAccessConfig(
                token=env_data.access_token.get_secret_value()
            ),
            http_path=env_data.http_path,
            server_hostname=env_data.server_hostname,
        ),
        upload_config=DatabricksDeltaTablesUploaderConfig(
            catalog=CATALOG, database="default", table_name=destination_table
        ),
    )
    with staged_path.open("r") as f:
        staged_data = json.load(f)
    expected_num_elements = len(staged_data)
    uploader.precheck()
    uploader.run(path=staged_path, file_data=mock_file_data)
    validate_destination(expected_num_elements=expected_num_elements, table_name=destination_table)
