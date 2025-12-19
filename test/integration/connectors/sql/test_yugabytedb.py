import json
from pathlib import Path

import pytest
from _pytest.fixtures import TopRequest
from psycopg2 import connect

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
    source_filedata_display_name_set_check,
)
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.sql.yugabytedb import (
    CONNECTOR_TYPE,
    YugabyteDbAccessConfig,
    YugabyteDbConnectionConfig,
    YugabyteDbDownloader,
    YugabyteDbDownloaderConfig,
    YugabyteDbIndexer,
    YugabyteDbIndexerConfig,
    YugabyteDbUploader,
    YugabyteDbUploadStager,
)

SEED_DATA_ROWS = 10


@pytest.fixture
def source_database_setup() -> str:
    db_name = "yugabyte"  # Use default yugabyte database
    with docker_compose_context(docker_compose_path=env_setup_path / "sql" / "yugabytedb" / "source"):
        # Table is already created by initial_scripts_dir in yugabyte database
        # Just connect and insert test data using YugabyteDB-specific psycopg2
        connection = connect(
            user="yugabyte",
            password="yugabyte",
            dbname=db_name,
            host="localhost",
            port=5433,
            # YugabyteDB-specific parameters
            load_balance="False",
            topology_keys="",
        )
        with connection.cursor() as cursor:
            # Insert test data
            for i in range(SEED_DATA_ROWS):
                sql_statment = f"INSERT INTO cars (brand, price) VALUES ('brand_{i}', {i})"
                cursor.execute(sql_statment)
            connection.commit()
        yield db_name


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, SQL_TAG)
async def test_yugabytedb_source(temp_dir: Path, source_database_setup: str):
    connect_params = {
        "host": "localhost",
        "port": 5433,
        "database": "yugabyte",  # Use default yugabyte database
        "user": "yugabyte",
        "password": "yugabyte",
        # YugabyteDB-specific parameters for load balancing and topology awareness
        "load_balance": "False",  # Disable load balancing for local testing
        "topology_keys": "",  # No topology keys for local testing
        "yb_servers_refresh_interval": 300,  # Refresh interval for server list
    }
    # Test YugabyteDB connector with its specific connection parameters
    connection_config = YugabyteDbConnectionConfig(
        host=connect_params["host"],
        port=connect_params["port"],
        database=connect_params["database"],
        username=connect_params["user"],
        access_config=YugabyteDbAccessConfig(password=connect_params["password"]),
        load_balance=connect_params["load_balance"],
        topology_keys=connect_params["topology_keys"],
        yb_servers_refresh_interval=connect_params["yb_servers_refresh_interval"],
    )
    indexer = YugabyteDbIndexer(
        connection_config=connection_config,
        index_config=YugabyteDbIndexerConfig(table_name="cars", id_column="car_id", batch_size=6),
    )
    downloader = YugabyteDbDownloader(
        connection_config=connection_config,
        download_config=YugabyteDbDownloaderConfig(fields=["car_id", "brand"], download_dir=temp_dir),
    )
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="yugabytedb",
            expected_num_files=SEED_DATA_ROWS,
            expected_number_indexed_file_data=2,
            validate_downloaded_files=True,
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
        ),
    )


def validate_destination(
    connect_params: dict,
    expected_num_elements: int,
    test_embedding: list[float],
    expected_text: str,
):
    # Run the following validations:
    # * Check that the number of records in the table match the expected value
    # * Given the embedding, make sure it matches the associated text it belongs to
    with connect(**connect_params) as connection:
        cursor = connection.cursor()
        query = "select count(*) from elements;"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        assert count == expected_num_elements, (
            f"dest check failed: got {count}, expected {expected_num_elements}"
        )

        cursor.execute("SELECT embeddings FROM elements order by text limit 1")
        similarity_query = (
            f"SELECT text FROM elements ORDER BY embeddings <-> '{test_embedding}' LIMIT 1;"
        )
        cursor.execute(similarity_query)
        res = cursor.fetchone()
        assert res[0] == expected_text


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
async def test_yugabytedb_destination(upload_file: Path, temp_dir: Path):
    # the yugabytedb destination connector doesn't leverage the file data but is required as an input,
    # mocking it with arbitrary values to meet the base requirements:
    mock_file_data = FileData(
        identifier="mock file data",
        connector_type=CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(filename=upload_file.name, fullpath=upload_file.name),
    )
    with docker_compose_context(
        docker_compose_path=env_setup_path / "sql" / "yugabytedb" / "destination"
    ):
        # Table and pgvector extension are already created by initial_scripts_dir in yugabyte database
        stager = YugabyteDbUploadStager()
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
            "port": 5433,
            "database": "yugabyte",  # Use default yugabyte database
            "user": "yugabyte",
            "password": "yugabyte",
            # YugabyteDB-specific parameters for load balancing and topology awareness
            "load_balance": "False",  # Disable load balancing for local testing
            "topology_keys": "",  # No topology keys for local testing
            "yb_servers_refresh_interval": 300,  # Refresh interval
        }

        # Test YugabyteDB connector with its specific connection parameters
        uploader = YugabyteDbUploader(
            connection_config=YugabyteDbConnectionConfig(
                host=connect_params["host"],
                port=connect_params["port"],
                database=connect_params["database"],
                username=connect_params["user"],
                access_config=YugabyteDbAccessConfig(password=connect_params["password"]),
                load_balance=connect_params["load_balance"],
                topology_keys=connect_params["topology_keys"],
                yb_servers_refresh_interval=connect_params["yb_servers_refresh_interval"],
            )
        )
        uploader.precheck()
        uploader.run(path=staged_path, file_data=mock_file_data)

        with staged_path.open("r") as f:
            staged_data = json.load(f)

        sample_element = staged_data[0]
        expected_num_elements = len(staged_data)
        validate_destination(
            connect_params=connect_params,
            expected_num_elements=expected_num_elements,
            expected_text=sample_element["text"],
            test_embedding=sample_element["embeddings"],
        )

        uploader.run(path=staged_path, file_data=mock_file_data)
        validate_destination(
            connect_params=connect_params,
            expected_num_elements=expected_num_elements,
            expected_text=sample_element["text"],
            test_embedding=sample_element["embeddings"],
        )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_yugabytedb_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = YugabyteDbUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )

