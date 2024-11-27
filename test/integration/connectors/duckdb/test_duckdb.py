import tempfile
from pathlib import Path
from contextlib import contextmanager
from typing import Generator

import pytest
import duckdb
import pandas as pd

from test.integration.connectors.utils.constants import DESTINATION_TAG, env_setup_path
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.duckdb.duckdb import (
    CONNECTOR_TYPE,
    DuckDBConnectionConfig,
    DuckDBUploadStager,
    DuckDBUploaderConfig,
    DuckDBUploader,
)


@contextmanager
def duckdbd_setup() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "temp_duck.db"
        db_init_path = env_setup_path / "duckdb" / "duckdb-schema.sql"
        assert db_init_path.exists()
        assert db_init_path.is_file()
        with duckdb.connect(database=db_path) as duckdb_connection:
            with db_init_path.open("r") as f:
                query = f.read()
            duckdb_connection.execute(query)
            duckdb_connection.close()
        yield db_path


def validate_duckdb_destination(db_path: Path, expected_num_elements: int):
    conn = None
    try:
        conn = duckdb.connect(db_path)
        _results = conn.sql("select count(*) from elements").fetchall()
        _count = _results[0][0]
        assert (
            _count == expected_num_elements
        ), f"dest check failed: got {_count}, expected {expected_num_elements}"
        conn.close()
    finally:
        if conn:
            conn.close()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "duckdb")
def test_duckdb_destination(upload_file: Path):
    with duckdbd_setup() as test_db_path:
        with tempfile.TemporaryDirectory() as temp_dir:
            file_data = FileData(
                source_identifiers=SourceIdentifiers(
                    fullpath=upload_file.name, filename=upload_file.name
                ),
                connector_type=CONNECTOR_TYPE,
                identifier="mock-file-data",
            )

            ## deafults to default stager config
            stager = DuckDBUploadStager()
            stager_params = {
                "elements_filepath": upload_file,
                "file_data": file_data,
                "output_dir": temp_dir,
                "output_filename": "test_db",
            }
            staged_path = stager.run(**stager_params)

            connection_config = DuckDBConnectionConfig(database=str(test_db_path))
            upload_config = DuckDBUploaderConfig()
            uploader = DuckDBUploader(
                connection_config=connection_config, upload_config=upload_config
            )

            uploader.run(path=staged_path, file_data=file_data)

            staged_df = pd.read_json(staged_path, orient="records", lines=True)
            validate_duckdb_destination(db_path=test_db_path, expected_num_elements=len(staged_df))
