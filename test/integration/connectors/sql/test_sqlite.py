import sqlite3
import tempfile
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pytest

from test.integration.connectors.utils.constants import DESTINATION_TAG, SOURCE_TAG, env_setup_path
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.v2.interfaces import FileData
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

SEED_DATA_ROWS = 20


@contextmanager
def sqlite_download_setup() -> Path:
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
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, "sql")
async def test_sqlite_source():
    with sqlite_download_setup() as db_path:
        with tempfile.TemporaryDirectory() as tmpdir:
            connection_config = SQLiteConnectionConfig(database_path=db_path)
            indexer = SQLiteIndexer(
                connection_config=connection_config,
                index_config=SQLiteIndexerConfig(
                    table_name="cars", id_column="car_id", batch_size=5
                ),
            )
            downloader = SQLiteDownloader(
                connection_config=connection_config,
                download_config=SQLiteDownloaderConfig(
                    fields=["car_id", "brand"], download_dir=Path(tmpdir)
                ),
            )
            await source_connector_validation(
                indexer=indexer,
                downloader=downloader,
                configs=ValidationConfigs(
                    test_id="sqlite",
                    expected_num_files=SEED_DATA_ROWS,
                    expected_number_indexed_file_data=4,
                    validate_downloaded_files=True,
                ),
            )


@contextmanager
def sqlite_upload_setup() -> Path:
    # Provision the local file that sqlite points to to have the desired schema for the integration
    # tests and make sure the file and connection get cleaned up by using a context manager.
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "elements.db"
        db_init_path = env_setup_path / "sql" / "sqlite" / "destination" / "sqlite-schema.sql"
        assert db_init_path.exists()
        assert db_init_path.is_file()
        with sqlite3.connect(database=db_path) as sqlite_connection:
            with db_init_path.open("r") as f:
                query = f.read()
            cursor = sqlite_connection.cursor()
            cursor.executescript(query)
        yield db_path


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
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "sql")
async def test_sqlite_destination(upload_file: Path):
    # the sqlite destination connector doesn't leverage the file data but is required as an input,
    # mocking it with arbitrary values to meet the base requirements:
    mock_file_data = FileData(identifier="mock file data", connector_type=CONNECTOR_TYPE)
    with sqlite_upload_setup() as db_path:
        with tempfile.TemporaryDirectory() as tmpdir:
            stager = SQLiteUploadStager()
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

            uploader = SQLiteUploader(
                connection_config=SQLiteConnectionConfig(database_path=db_path)
            )
            uploader.run(path=staged_path, file_data=mock_file_data)

            staged_df = pd.read_json(staged_path, orient="records", lines=True)
            validate_destination(db_path=db_path, expected_num_elements=len(staged_df))

            uploader.run(path=staged_path, file_data=mock_file_data)
            validate_destination(db_path=db_path, expected_num_elements=len(staged_df))
