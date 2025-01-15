import os
import uuid
from pathlib import Path
from typing import Generator

import duckdb
import pandas as pd
import pytest

from test.integration.connectors.utils.constants import DESTINATION_TAG, SQL_TAG
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.duckdb.motherduck import (
    CONNECTOR_TYPE,
    MotherDuckAccessConfig,
    MotherDuckConnectionConfig,
    MotherDuckUploader,
    MotherDuckUploaderConfig,
    MotherDuckUploadStager,
)


@pytest.fixture
def md_token() -> str:
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN", None)
    assert motherduck_token
    return motherduck_token


@pytest.fixture
def provisioned_db(md_token: str, duckdb_schema: Path) -> Generator[str, None, None]:
    database_name = f"test_{str(uuid.uuid4()).replace('-', '_')}"
    try:
        with duckdb.connect(f"md:?motherduck_token={md_token}") as md_conn:
            with duckdb_schema.open("r") as f:
                query = f.read()
            md_conn.execute(f"CREATE DATABASE {database_name}")
            md_conn.execute(f"USE {database_name}")
            md_conn.execute(query)
            md_conn.close()
        yield database_name
    finally:
        with duckdb.connect(f"md:?motherduck_token={md_token}") as md_conn:
            md_conn.execute(f"DROP DATABASE {database_name}")
            md_conn.close()


def validate_motherduck_destination(database: str, expected_num_elements: int, md_token: str):
    conn = None
    try:
        conn = duckdb.connect(f"md:?motherduck_token={md_token}")
        conn.execute(f"USE {database}")
        _results = conn.sql("select count(*) from elements").fetchall()
        _count = _results[0][0]
        assert (
            _count == expected_num_elements
        ), f"dest check failed: got {_count}, expected {expected_num_elements}"
        conn.close()
    finally:
        if conn:
            conn.close()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env("MOTHERDUCK_TOKEN")
def test_motherduck_destination(
    md_token: str, upload_file: Path, provisioned_db: str, temp_dir: Path
):
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock-file-data",
    )

    stager = MotherDuckUploadStager()
    staged_path = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )

    access_config = MotherDuckAccessConfig(md_token=md_token)
    connection_config = MotherDuckConnectionConfig(
        database=provisioned_db, access_config=access_config
    )
    upload_config = MotherDuckUploaderConfig()
    uploader = MotherDuckUploader(connection_config=connection_config, upload_config=upload_config)

    uploader.run(path=staged_path, file_data=file_data)

    staged_df = pd.read_json(staged_path, orient="records", lines=True)
    validate_motherduck_destination(
        database=provisioned_db, expected_num_elements=len(staged_df), md_token=md_token
    )
