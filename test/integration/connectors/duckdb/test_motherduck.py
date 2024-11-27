import os
import tempfile
import uuid
from pathlib import Path
from contextlib import contextmanager
from typing import Generator

import pytest
import duckdb
import pandas as pd

from test.integration.utils import requires_env
from test.integration.connectors.utils.constants import DESTINATION_TAG, env_setup_path
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.duckdb.motherduck import (
    CONNECTOR_TYPE,
    MotherDuckAccessConfig,
    MotherDuckConnectionConfig,
    MotherDuckUploadStager,
    MotherDuckUploaderConfig,
    MotherDuckUploader,
)


@contextmanager
def motherduck_setup(md_token: str) -> Generator[Path, None, None]:
    database_name = f"test_{str(uuid.uuid4()).replace('-', '_')}"
    try:
        db_init_path = env_setup_path / "duckdb" / "duckdb-schema.sql"
        assert db_init_path.exists()
        assert db_init_path.is_file()
        with duckdb.connect(f"md:?motherduck_token={md_token}") as md_conn:
            with db_init_path.open("r") as f:
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


def get_motherduck_token() -> dict:
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN", None)
    assert motherduck_token
    return motherduck_token


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "motherduck")
@requires_env("MOTHERDUCK_TOKEN")
def test_motherduck_destination(upload_file: Path):
    md_token = get_motherduck_token()
    with motherduck_setup(md_token) as test_database:
        with tempfile.TemporaryDirectory() as temp_dir:
            file_data = FileData(
                source_identifiers=SourceIdentifiers(
                    fullpath=upload_file.name, filename=upload_file.name
                ),
                connector_type=CONNECTOR_TYPE,
                identifier="mock-file-data",
            )

            ## deafults to default stager config
            stager = MotherDuckUploadStager()
            stager_params = {
                "elements_filepath": upload_file,
                "file_data": file_data,
                "output_dir": temp_dir,
                "output_filename": "test_db",
            }
            staged_path = stager.run(**stager_params)

            access_config = MotherDuckAccessConfig(md_token=md_token)
            connection_config = MotherDuckConnectionConfig(
                database=test_database, access_config=access_config
            )
            upload_config = MotherDuckUploaderConfig()
            uploader = MotherDuckUploader(
                connection_config=connection_config, upload_config=upload_config
            )

            uploader.run(path=staged_path, file_data=file_data)

            staged_df = pd.read_json(staged_path, orient="records", lines=True)
            validate_motherduck_destination(
                database=test_database, expected_num_elements=len(staged_df), md_token=md_token
            )
