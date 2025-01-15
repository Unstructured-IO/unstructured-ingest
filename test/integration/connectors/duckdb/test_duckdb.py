import json
from pathlib import Path

import duckdb
import pytest
from _pytest.fixtures import TopRequest

from test.integration.connectors.utils.constants import DESTINATION_TAG, SQL_TAG
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.duckdb.duckdb import (
    CONNECTOR_TYPE,
    DuckDBConnectionConfig,
    DuckDBUploader,
    DuckDBUploaderConfig,
    DuckDBUploadStager,
)


@pytest.fixture
def provisioned_db_file(duckdb_schema: Path, temp_dir: Path) -> Path:
    db_path = Path(temp_dir) / "temp_duck.db"
    with duckdb.connect(database=db_path) as duckdb_connection:
        with duckdb_schema.open("r") as f:
            query = f.read()
        duckdb_connection.execute(query)
        duckdb_connection.close()
    return db_path


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


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "duckdb", SQL_TAG)
def test_duckdb_destination(upload_file: Path, provisioned_db_file: Path, temp_dir: Path):
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock-file-data",
    )

    stager = DuckDBUploadStager()
    staged_path = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )

    connection_config = DuckDBConnectionConfig(database=str(provisioned_db_file))
    upload_config = DuckDBUploaderConfig()
    uploader = DuckDBUploader(connection_config=connection_config, upload_config=upload_config)

    uploader.run(path=staged_path, file_data=file_data)

    with staged_path.open() as f:
        data = json.load(f)
    validate_duckdb_destination(db_path=provisioned_db_file, expected_num_elements=len(data))


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "duckdb", SQL_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_duckdb_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = DuckDBUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
