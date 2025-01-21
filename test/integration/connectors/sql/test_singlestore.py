import json
from pathlib import Path

import pytest
import singlestoredb as s2
from _pytest.fixtures import TopRequest

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
    SOURCE_TAG,
    SQL_TAG,
    env_setup_path,
)
from test.integration.connectors.utils.docker_compose import docker_compose_context
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
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

SEED_DATA_ROWS = 10


@pytest.fixture
def source_database_setup() -> dict:
    connect_params = {
        "host": "localhost",
        "port": 3306,
        "database": "ingest_test",
        "user": "root",
        "password": "password",
    }
    with docker_compose_context(
        docker_compose_path=env_setup_path / "sql" / "singlestore" / "source"
    ):
        with s2.connect(**connect_params) as connection:
            with connection.cursor() as cursor:
                for i in range(SEED_DATA_ROWS):
                    sql_statment = f"INSERT INTO cars (brand, price) VALUES " f"('brand_{i}', {i})"
                    cursor.execute(sql_statment)
                connection.commit()
        yield connect_params


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, SQL_TAG)
async def test_singlestore_source(temp_dir: Path, source_database_setup: dict):

    connection_config = SingleStoreConnectionConfig(
        host=source_database_setup["host"],
        port=source_database_setup["port"],
        database=source_database_setup["database"],
        user=source_database_setup["user"],
        access_config=SingleStoreAccessConfig(password=source_database_setup["password"]),
    )
    indexer = SingleStoreIndexer(
        connection_config=connection_config,
        index_config=SingleStoreIndexerConfig(table_name="cars", id_column="car_id", batch_size=6),
    )
    downloader = SingleStoreDownloader(
        connection_config=connection_config,
        download_config=SingleStoreDownloaderConfig(
            fields=["car_id", "brand"], download_dir=temp_dir
        ),
    )
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="singlestore",
            expected_num_files=SEED_DATA_ROWS,
            expected_number_indexed_file_data=2,
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
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
async def test_singlestore_destination(upload_file: Path, temp_dir: Path):
    mock_file_data = FileData(
        identifier="mock file data",
        connector_type=CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(filename=upload_file.name, fullpath=upload_file.name),
    )
    with docker_compose_context(
        docker_compose_path=env_setup_path / "sql" / "singlestore" / "destination"
    ):
        stager = SingleStoreUploadStager()
        staged_path = stager.run(
            elements_filepath=upload_file,
            file_data=mock_file_data,
            output_dir=temp_dir,
            output_filename=upload_file.name,
        )

        # The stager should append the `.json` suffix to the output filename passed in.
        assert staged_path.suffix == upload_file.suffix

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
def test_singlestore_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = SingleStoreUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
