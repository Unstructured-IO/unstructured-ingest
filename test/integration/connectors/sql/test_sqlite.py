import json
import sqlite3
import tempfile
from pathlib import Path

import pytest
from _pytest.fixtures import TopRequest

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
    SOURCE_TAG,
    SQL_TAG,
    env_setup_path,
)
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.sql.sqlite import (
    CONNECTOR_TYPE,
    SQLiteConnectionConfig,
    SQLiteDownloader,
    SQLiteDownloaderConfig,
    SQLiteIndexer,
    SQLiteIndexerConfig,
    SQLiteUploader,
    SQLiteUploadStager,
)

SEED_DATA_ROWS = 10


@pytest.fixture
def source_database_setup() -> Path:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "mock_database.db"
        db_init_path = env_setup_path / "sql" / "sqlite" / "source" / "sqlite-schema.sql"
        assert db_init_path.exists()
        assert db_init_path.is_file()
        with sqlite3.connect(database=db_path) as sqlite_connection:
            cursor = sqlite_connection.cursor()
            with db_init_path.open("r") as f:
                query = f.read()
            cursor.executescript(query)
            for i in range(SEED_DATA_ROWS):
                sql_statment = f"INSERT INTO cars (brand, price) " f"VALUES ('brand{i}', {i})"
                cursor.execute(sql_statment)

            sqlite_connection.commit()
            cursor.close()
        yield db_path


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, SQL_TAG)
async def test_sqlite_source(source_database_setup: Path, temp_dir: Path):
    connection_config = SQLiteConnectionConfig(database_path=source_database_setup)
    indexer = SQLiteIndexer(
        connection_config=connection_config,
        index_config=SQLiteIndexerConfig(table_name="cars", id_column="car_id", batch_size=6),
    )
    downloader = SQLiteDownloader(
        connection_config=connection_config,
        download_config=SQLiteDownloaderConfig(fields=["car_id", "brand"], download_dir=temp_dir),
    )
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="sqlite",
            expected_num_files=SEED_DATA_ROWS,
            expected_number_indexed_file_data=2,
            validate_downloaded_files=True,
        ),
    )


@pytest.fixture
def destination_database_setup(temp_dir: Path) -> Path:
    # Provision the local file that sqlite points to to have the desired schema for the integration
    # tests and make sure the file and connection get cleaned up by using a context manager.
    db_path = temp_dir / "elements.db"
    db_init_path = env_setup_path / "sql" / "sqlite" / "destination" / "sqlite-schema.sql"
    assert db_init_path.exists()
    assert db_init_path.is_file()
    with sqlite3.connect(database=db_path) as sqlite_connection:
        with db_init_path.open("r") as f:
            query = f.read()
        cursor = sqlite_connection.cursor()
        cursor.executescript(query)
    return db_path


def validate_destination(db_path: Path, expected_num_elements: int):
    # Run the following validations:
    # * Check that the number of records in the table match the expected value
    connection = None
    try:
        connection = sqlite3.connect(database=db_path)
        query = "select count(*) from elements;"
        cursor = connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        assert (
            count == expected_num_elements
        ), f"dest check failed: got {count}, expected {expected_num_elements}"
    finally:
        if connection:
            connection.close()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
async def test_sqlite_destination(
    upload_file: Path, temp_dir: Path, destination_database_setup: Path
):
    # the sqlite destination connector doesn't leverage the file data but is required as an input,
    # mocking it with arbitrary values to meet the base requirements:
    mock_file_data = FileData(
        identifier="mock file data",
        connector_type=CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(filename=upload_file.name, fullpath=upload_file.name),
    )
    stager = SQLiteUploadStager()
    staged_path = stager.run(
        elements_filepath=upload_file,
        file_data=mock_file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )

    # The stager should append the `.json` suffix to the output filename passed in.
    assert staged_path.suffix == upload_file.suffix

    uploader = SQLiteUploader(
        connection_config=SQLiteConnectionConfig(database_path=destination_database_setup)
    )
    uploader.precheck()
    uploader.run(path=staged_path, file_data=mock_file_data)

    with staged_path.open("r") as f:
        staged_data = json.load(f)
    validate_destination(db_path=destination_database_setup, expected_num_elements=len(staged_data))

    uploader.run(path=staged_path, file_data=mock_file_data)
    validate_destination(db_path=destination_database_setup, expected_num_elements=len(staged_data))


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_sqlite_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = SQLiteUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
