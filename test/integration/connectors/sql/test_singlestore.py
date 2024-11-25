import tempfile
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pytest
import singlestoredb as s2

from test.integration.connectors.utils.constants import DESTINATION_TAG, SOURCE_TAG, env_setup_path
from test.integration.connectors.utils.docker_compose import docker_compose_context
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.v2.interfaces import FileData
from unstructured_ingest.v2.processes.connectors.sql.singlestore import (
    CONNECTOR_TYPE,
    SingleStoreAccessConfig,
    SingleStoreConnectionConfig,
    SingleStoreDownloader,
    SingleStoreDownloaderConfig,
    SingleStoreIndexer,
    SingleStoreIndexerConfig,
    SingleStoreUploader,
    SingleStoreUploaderConfig,
    SingleStoreUploadStager,
)

SEED_DATA_ROWS = 20


@contextmanager
def singlestore_download_setup(connect_params: dict) -> None:
    with docker_compose_context(
        docker_compose_path=env_setup_path / "sql" / "singlestore" / "source"
    ):
        with s2.connect(**connect_params) as connection:
            with connection.cursor() as cursor:
                for i in range(SEED_DATA_ROWS):
                    sql_statment = f"INSERT INTO cars (brand, price) VALUES " f"('brand_{i}', {i})"
                    cursor.execute(sql_statment)
                connection.commit()
        yield


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, "sql")
async def test_singlestore_source():
    connect_params = {
        "host": "localhost",
        "port": 3306,
        "database": "ingest_test",
        "user": "root",
        "password": "password",
    }
    with singlestore_download_setup(connect_params=connect_params):
        with tempfile.TemporaryDirectory() as tmpdir:
            connection_config = SingleStoreConnectionConfig(
                host=connect_params["host"],
                port=connect_params["port"],
                database=connect_params["database"],
                user=connect_params["user"],
                access_config=SingleStoreAccessConfig(password=connect_params["password"]),
            )
            indexer = SingleStoreIndexer(
                connection_config=connection_config,
                index_config=SingleStoreIndexerConfig(
                    table_name="cars", id_column="car_id", batch_size=5
                ),
            )
            downloader = SingleStoreDownloader(
                connection_config=connection_config,
                download_config=SingleStoreDownloaderConfig(
                    fields=["car_id", "brand"], download_dir=Path(tmpdir)
                ),
            )
            await source_connector_validation(
                indexer=indexer,
                downloader=downloader,
                configs=ValidationConfigs(
                    test_id="singlestore",
                    expected_num_files=SEED_DATA_ROWS,
                    expected_number_indexed_file_data=4,
                    validate_downloaded_files=True,
                ),
            )


def validate_destination(
    connect_params: dict,
    expected_num_elements: int,
):
    with s2.connect(**connect_params) as connection:
        with connection.cursor() as cursor:
            query = "select count(*) from elements;"
            cursor.execute(query)
            count = cursor.fetchone()[0]
            assert (
                count == expected_num_elements
            ), f"dest check failed: got {count}, expected {expected_num_elements}"


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "sql")
async def test_singlestore_destination(upload_file: Path):
    mock_file_data = FileData(identifier="mock file data", connector_type=CONNECTOR_TYPE)
    with docker_compose_context(
        docker_compose_path=env_setup_path / "sql" / "singlestore" / "destination"
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            stager = SingleStoreUploadStager()
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
                "host": "localhost",
                "port": 3306,
                "database": "ingest_test",
                "user": "root",
                "password": "password",
            }

            uploader = SingleStoreUploader(
                connection_config=SingleStoreConnectionConfig(
                    host=connect_params["host"],
                    port=connect_params["port"],
                    database=connect_params["database"],
                    user=connect_params["user"],
                    access_config=SingleStoreAccessConfig(password=connect_params["password"]),
                ),
                upload_config=SingleStoreUploaderConfig(
                    table_name="elements",
                ),
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
