import json
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest
from _pytest.fixtures import TopRequest
from opensearchpy import Document, Keyword, OpenSearch, Text

from test.integration.connectors.utils.constants import DESTINATION_TAG, NOSQL_TAG, SOURCE_TAG
from test.integration.connectors.utils.docker import HealthCheck, container_context
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
)
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.elasticsearch.opensearch import (
    CONNECTOR_TYPE,
    OpenSearchAccessConfig,
    OpenSearchConnectionConfig,
    OpenSearchDownloader,
    OpenSearchDownloaderConfig,
    OpenSearchIndexer,
    OpenSearchIndexerConfig,
    OpenSearchUploader,
    OpenSearchUploaderConfig,
    OpenSearchUploadStager,
    OpenSearchUploadStagerConfig,
)

SOURCE_INDEX_NAME = "movies"
DESTINATION_INDEX_NAME = "elements"


class Movie(Document):
    title = Text(fields={"raw": Keyword()})
    year = Text()
    director = Text()
    cast = Text()
    genre = Text()
    wiki_page = Text()
    ethnicity = Text()
    plot = Text()

    class Index:
        name = SOURCE_INDEX_NAME

    def save(self, **kwargs):
        return super(Movie, self).save(**kwargs)


@contextmanager
def get_client() -> Generator[OpenSearch, None, None]:
    with OpenSearch(
        hosts=[{"host": "localhost", "port": 9200}],
        http_auth=("admin", "admin"),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False,
    ) as client:
        yield client


def get_index_count(client: OpenSearch, index_name: str) -> int:
    count_resp = client.cat.count(index=index_name, params={"format": "json"})
    return int(count_resp[0]["count"])


def wait_for_write(
    client: OpenSearch, index_name: str, expected_count: int, timeout: int = 30, interval: int = 1
) -> None:
    current_count = get_index_count(client, index_name)
    start = time.time()
    while time.time() - start < timeout:
        print(f"waiting for current count ({current_count}) to match expected {expected_count}")
        time.sleep(interval)
        current_count = get_index_count(client, index_name)
        if current_count == expected_count:
            return
    raise TimeoutError("Timed out while waiting for write to sync")


def validate_count(
    client: OpenSearch, index_name: str, expected_count: int, retries: int = 10, interval: int = 1
) -> None:
    current_count = get_index_count(client, index_name)
    if current_count == expected_count:
        return
    tries = 0
    while tries < retries:
        print(
            f"retrying validation to check if expected count "
            f"{expected_count} will match current count {current_count}"
        )
        time.sleep(interval)
        current_count = get_index_count(client, index_name)
        if current_count == expected_count:
            break
    assert current_count == expected_count, (
        f"Expected count ({expected_count}) doesn't match how "
        f"much came back from index: {current_count}"
    )


@pytest.fixture
def source_index(movies_dataframe: pd.DataFrame) -> str:
    with container_context(
        image="opensearchproject/opensearch:2.11.1",
        ports={9200: 9200, 9600: 9600},
        environment={"discovery.type": "single-node"},
        healthcheck=HealthCheck(
            test="curl --fail https://localhost:9200/_cat/health -ku 'admin:admin' >/dev/null || exit 1",  # noqa: E501
            interval=1,
        ),
    ):
        with get_client() as client:
            Movie.init(using=client)
            for i, row in movies_dataframe.iterrows():
                movie = Movie(
                    meta={"id": i},
                    title=row["Title"],
                    year=row["Release Year"],
                    director=row["Director"],
                    cast=row["Cast"],
                    genre=row["Genre"],
                    wiki_page=row["Wiki Page"],
                    ethnicity=row["Origin/Ethnicity"],
                    plot=row["Plot"],
                )
                movie.save(using=client)
            wait_for_write(
                client=client, index_name=SOURCE_INDEX_NAME, expected_count=len(movies_dataframe)
            )
        yield SOURCE_INDEX_NAME


@pytest.fixture
def destination_index(opensearch_elements_mapping: dict) -> str:
    with container_context(
        image="opensearchproject/opensearch:2.11.1",
        ports={9200: 9200, 9600: 9600},
        environment={"discovery.type": "single-node"},
        healthcheck=HealthCheck(
            test="curl --fail https://localhost:9200/_cat/health -ku 'admin:admin' >/dev/null || exit 1",  # noqa: E501
            interval=1,
        ),
    ):
        with get_client() as client:
            response = client.indices.create(
                index=DESTINATION_INDEX_NAME, body=opensearch_elements_mapping
            )
            if not response["acknowledged"]:
                raise RuntimeError(f"failed to create index: {response}")
        yield DESTINATION_INDEX_NAME


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG)
async def test_opensearch_source(source_index: str, movies_dataframe: pd.DataFrame):
    indexer_config = OpenSearchIndexerConfig(index_name=source_index)
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        connection_config = OpenSearchConnectionConfig(
            access_config=OpenSearchAccessConfig(password="admin"),
            username="admin",
            hosts=["http://localhost:9200"],
            use_ssl=True,
        )
        download_config = OpenSearchDownloaderConfig(download_dir=tempdir_path)
        indexer = OpenSearchIndexer(
            connection_config=connection_config, index_config=indexer_config
        )
        downloader = OpenSearchDownloader(
            connection_config=connection_config, download_config=download_config
        )
        expected_num_files = len(movies_dataframe)
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=SourceValidationConfigs(
                test_id=CONNECTOR_TYPE,
                expected_num_files=expected_num_files,
                expected_number_indexed_file_data=1,
                validate_downloaded_files=True,
            ),
        )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG)
def test_opensearch_source_precheck_fail_no_cluster():
    indexer_config = OpenSearchIndexerConfig(index_name="index")

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(password="admin"),
        username="admin",
        hosts=["http://localhost:9200"],
        use_ssl=True,
    )
    indexer = OpenSearchIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG)
def test_opensearch_source_precheck_fail_no_index(source_index: str):
    indexer_config = OpenSearchIndexerConfig(index_name="index")

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(password="admin"),
        username="admin",
        hosts=["http://localhost:9200"],
        use_ssl=True,
    )
    indexer = OpenSearchIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
async def test_opensearch_destination(
    upload_file: Path,
    destination_index: str,
    tmp_path: Path,
):
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(password="admin"),
        username="admin",
        hosts=["http://localhost:9200"],
        use_ssl=True,
    )
    stager = OpenSearchUploadStager(
        upload_stager_config=OpenSearchUploadStagerConfig(index_name=destination_index)
    )

    uploader = OpenSearchUploader(
        connection_config=connection_config,
        upload_config=OpenSearchUploaderConfig(index_name=destination_index),
    )
    staged_filepath = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )
    uploader.precheck()
    uploader.run(path=staged_filepath, file_data=file_data)

    # Run validation
    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    expected_count = len(staged_elements)
    with get_client() as client:
        validate_count(client=client, expected_count=expected_count, index_name=destination_index)

    # Rerun and make sure the same documents get updated
    uploader.run(path=staged_filepath, file_data=file_data)
    with get_client() as client:
        validate_count(client=client, expected_count=expected_count, index_name=destination_index)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_opensearch_destination_precheck_fail():
    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(password="admin"),
        username="admin",
        hosts=["http://localhost:9200"],
        use_ssl=True,
    )
    uploader = OpenSearchUploader(
        connection_config=connection_config,
        upload_config=OpenSearchUploaderConfig(index_name="index"),
    )
    with pytest.raises(DestinationConnectionError):
        uploader.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
def test_opensearch_destination_precheck_fail_no_index(destination_index: str):
    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(password="admin"),
        username="admin",
        hosts=["http://localhost:9200"],
        use_ssl=True,
    )
    uploader = OpenSearchUploader(
        connection_config=connection_config,
        upload_config=OpenSearchUploaderConfig(index_name="index"),
    )
    with pytest.raises(DestinationConnectionError):
        uploader.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_opensearch_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = OpenSearchUploadStager(
        upload_stager_config=OpenSearchUploadStagerConfig(index_name="mock_index")
    )
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
