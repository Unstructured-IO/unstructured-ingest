import json
import os
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
    source_filedata_display_name_set_check,
)
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
)
from unstructured_ingest.processes.connectors.elasticsearch.opensearch import (
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
    detect_aws_opensearch_config,
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

        # Wrap precheck to run in thread pool to avoid event loop conflict with asyncio.run()
        import concurrent.futures

        original_precheck = indexer.precheck

        def threaded_precheck():
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(original_precheck)
                future.result()

        indexer.precheck = threaded_precheck

        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=SourceValidationConfigs(
                test_id=CONNECTOR_TYPE,
                expected_num_files=expected_num_files,
                expected_number_indexed_file_data=1,
                validate_downloaded_files=True,
                predownload_file_data_check=source_filedata_display_name_set_check,
                postdownload_file_data_check=source_filedata_display_name_set_check,
                exclude_fields_extend=["display_name"],  # includes dynamic ids, might change
            ),
        )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG)
async def test_opensearch_source_empty_fields(source_index: str, movies_dataframe: pd.DataFrame):
    """Test that empty fields list works without timeout (fixes AWS OpenSearch FGAC issue)."""
    indexer_config = OpenSearchIndexerConfig(index_name=source_index)
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        connection_config = OpenSearchConnectionConfig(
            access_config=OpenSearchAccessConfig(password="admin"),
            username="admin",
            hosts=["http://localhost:9200"],
            use_ssl=True,
        )
        download_config = OpenSearchDownloaderConfig(
            download_dir=tempdir_path,
            fields=[],  # Empty fields should omit _source
        )
        indexer = OpenSearchIndexer(
            connection_config=connection_config, index_config=indexer_config
        )
        downloader = OpenSearchDownloader(
            connection_config=connection_config, download_config=download_config
        )

        expected_num_files = len(movies_dataframe)

        # Wrap precheck to run in thread pool to avoid event loop conflict with asyncio.run()
        import concurrent.futures

        original_precheck = indexer.precheck

        def threaded_precheck():
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(original_precheck)
                future.result()

        indexer.precheck = threaded_precheck

        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=SourceValidationConfigs(
                test_id=CONNECTOR_TYPE,
                expected_num_files=expected_num_files,
                expected_number_indexed_file_data=1,
                validate_downloaded_files=True,
                predownload_file_data_check=source_filedata_display_name_set_check,
                postdownload_file_data_check=source_filedata_display_name_set_check,
                exclude_fields_extend=["display_name"],
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

    # Run precheck in thread pool to avoid event loop conflict with asyncio.run()
    import concurrent.futures

    def threaded_precheck():
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(uploader.precheck)
            future.result()

    threaded_precheck()
    await uploader.run_async(path=staged_filepath, file_data=file_data)

    # Run validation
    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    expected_count = len(staged_elements)
    with get_client() as client:
        validate_count(client=client, expected_count=expected_count, index_name=destination_index)

    # Rerun and make sure the same documents get updated
    await uploader.run_async(path=staged_filepath, file_data=file_data)
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


# AWS IAM Authentication Tests
# These tests require AWS credentials to be set in environment variables


@pytest.fixture
def aws_credentials():
    """Fixture that provides AWS credentials from environment variables."""
    aws_access_key_id = os.getenv("OPENSEARCH_AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("OPENSEARCH_AWS_SECRET_ACCESS_KEY")
    aws_host = os.getenv("OPENSEARCH_AWS_HOST")

    if not all([aws_access_key_id, aws_secret_access_key, aws_host]):
        pytest.skip(
            "AWS OpenSearch credentials not available. Set OPENSEARCH_AWS_ACCESS_KEY_ID, "
            "OPENSEARCH_AWS_SECRET_ACCESS_KEY, and OPENSEARCH_AWS_HOST environment variables."
        )

    return {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "aws_host": aws_host,
    }


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG, "aws", "iam")
async def test_opensearch_source_with_iam(aws_credentials: dict):
    """Test OpenSearch source connector with AWS IAM authentication."""
    indexer_config = OpenSearchIndexerConfig(index_name="opensearch_e2e_source")

    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        connection_config = OpenSearchConnectionConfig(
            access_config=OpenSearchAccessConfig(
                aws_access_key_id=aws_credentials["aws_access_key_id"],
                aws_secret_access_key=aws_credentials["aws_secret_access_key"],
            ),
            hosts=[aws_credentials["aws_host"]],
            use_ssl=True,
            verify_certs=True,
        )
        download_config = OpenSearchDownloaderConfig(download_dir=tempdir_path)

        indexer = OpenSearchIndexer(
            connection_config=connection_config, index_config=indexer_config
        )
        downloader = OpenSearchDownloader(
            connection_config=connection_config, download_config=download_config
        )

        # Wrap precheck to run in thread pool to avoid event loop conflict with asyncio.run()
        import concurrent.futures

        original_precheck = indexer.precheck

        def threaded_precheck():
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(original_precheck)
                future.result()

        indexer.precheck = threaded_precheck

        # Run source validation
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=SourceValidationConfigs(
                test_id=f"{CONNECTOR_TYPE}_iam",  # Use separate fixtures for IAM
                expected_num_files=10,  # AWS index has 10 documents
                expected_number_indexed_file_data=1,
                validate_downloaded_files=False,  # Skip fixture validation (focus on IAM auth)
                validate_file_data=False,  # Skip fixture validation (focus on IAM auth)
                predownload_file_data_check=source_filedata_display_name_set_check,
                postdownload_file_data_check=source_filedata_display_name_set_check,
                exclude_fields_extend=["display_name"],
            ),
        )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG, "aws", "iam")
async def test_opensearch_destination_with_iam(
    upload_file: Path,
    tmp_path: Path,
    aws_credentials: dict,
):
    """Test OpenSearch destination connector with AWS IAM authentication."""
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data iam test",
    )

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(
            aws_access_key_id=aws_credentials["aws_access_key_id"],
            aws_secret_access_key=aws_credentials["aws_secret_access_key"],
        ),
        hosts=[aws_credentials["aws_host"]],
        use_ssl=True,
        verify_certs=True,
    )

    stager = OpenSearchUploadStager(
        upload_stager_config=OpenSearchUploadStagerConfig(index_name="opensearch_e2e_dest")
    )

    uploader = OpenSearchUploader(
        connection_config=connection_config,
        upload_config=OpenSearchUploaderConfig(index_name="opensearch_e2e_dest"),
    )

    # Stage the file
    staged_filepath = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )

    # Run precheck in thread pool to avoid event loop conflict with asyncio.run()
    import concurrent.futures

    def threaded_precheck():
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(uploader.precheck)
            future.result()

    threaded_precheck()

    # Upload with IAM auth
    await uploader.run_async(path=staged_filepath, file_data=file_data)

    # Note: Validation against AWS OpenSearch would require async client
    # For now, if upload doesn't raise an exception, it's considered successful


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG, "aws", "iam")
def test_opensearch_source_iam_precheck_validates_credentials(aws_credentials: dict):
    """Test that precheck properly validates IAM credentials and connection."""
    indexer_config = OpenSearchIndexerConfig(index_name="opensearch_e2e_source")

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(
            aws_access_key_id=aws_credentials["aws_access_key_id"],
            aws_secret_access_key=aws_credentials["aws_secret_access_key"],
        ),
        hosts=[aws_credentials["aws_host"]],
        use_ssl=True,
        verify_certs=True,
    )

    indexer = OpenSearchIndexer(connection_config=connection_config, index_config=indexer_config)

    # Should succeed with valid credentials
    indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG, "aws", "iam")
def test_opensearch_source_iam_precheck_fail_invalid_credentials():
    """Test that precheck fails with invalid IAM credentials."""
    indexer_config = OpenSearchIndexerConfig(index_name="opensearch_e2e_source")

    # Skip if no AWS host available
    aws_host = os.getenv("OPENSEARCH_AWS_HOST")
    if not aws_host:
        pytest.skip("OPENSEARCH_AWS_HOST not set")

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(
            aws_access_key_id="INVALID_KEY",
            aws_secret_access_key="INVALID_SECRET",
        ),
        hosts=[aws_host],
        use_ssl=True,
        verify_certs=True,
    )

    indexer = OpenSearchIndexer(connection_config=connection_config, index_config=indexer_config)

    # Should fail with invalid credentials
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG, "aws", "iam")
def test_opensearch_destination_iam_precheck_fail_invalid_credentials():
    """Test that uploader precheck fails with invalid IAM credentials."""
    # Skip if no AWS host available
    aws_host = os.getenv("OPENSEARCH_AWS_HOST")
    if not aws_host:
        pytest.skip("OPENSEARCH_AWS_HOST not set")

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(
            aws_access_key_id="INVALID_KEY",
            aws_secret_access_key="INVALID_SECRET",
        ),
        hosts=[aws_host],
        use_ssl=True,
        verify_certs=True,
    )

    uploader = OpenSearchUploader(
        connection_config=connection_config,
        upload_config=OpenSearchUploaderConfig(index_name="opensearch_e2e_dest"),
    )

    # Should fail with invalid credentials
    with pytest.raises(DestinationConnectionError):
        uploader.precheck()


# AWS Hostname Detection Regex Tests
# These tests verify the regex patterns used to auto-detect AWS region and service


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@pytest.mark.parametrize(
    ("hostname", "expected_region", "expected_service"),
    [
        # Standard US regions - OpenSearch Service
        ("https://search-domain.us-east-1.es.amazonaws.com", "us-east-1", "es"),
        ("https://search-domain.us-west-2.es.amazonaws.com", "us-west-2", "es"),
        # EU regions
        ("https://search-domain.eu-west-1.es.amazonaws.com", "eu-west-1", "es"),
        ("https://search-domain.eu-central-1.es.amazonaws.com", "eu-central-1", "es"),
        # Asia Pacific regions
        ("https://search-domain.ap-southeast-1.es.amazonaws.com", "ap-southeast-1", "es"),
        ("https://search-domain.ap-northeast-1.es.amazonaws.com", "ap-northeast-1", "es"),
        # GovCloud regions
        ("https://search-domain.us-gov-west-1.es.amazonaws.com", "us-gov-west-1", "es"),
        ("https://search-domain.us-gov-east-1.es.amazonaws.com", "us-gov-east-1", "es"),
        # China regions
        ("https://search-domain.cn-north-1.es.amazonaws.com", "cn-north-1", "es"),
        ("https://search-domain.cn-northwest-1.es.amazonaws.com", "cn-northwest-1", "es"),
        # OpenSearch Serverless (AOSS)
        ("https://abc123xyz.us-east-1.aoss.amazonaws.com", "us-east-1", "aoss"),
        ("https://abc123xyz.eu-west-1.aoss.amazonaws.com", "eu-west-1", "aoss"),
        ("https://abc123xyz.us-gov-west-1.aoss.amazonaws.com", "us-gov-west-1", "aoss"),
        # Without https://
        ("search-domain.us-east-1.es.amazonaws.com", "us-east-1", "es"),
        # With port
        ("https://search-domain.us-east-1.es.amazonaws.com:443", "us-east-1", "es"),
    ],
)
def test_detect_aws_opensearch_config_valid(hostname, expected_region, expected_service):
    """Test AWS hostname regex patterns detect valid AWS OpenSearch hostnames."""
    result = detect_aws_opensearch_config(hostname)
    assert result is not None, f"Failed to detect AWS config from {hostname}"
    region, service = result
    assert region == expected_region
    assert service == expected_service


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@pytest.mark.parametrize(
    "hostname",
    [
        "https://localhost:9200",
        "https://my-opensearch.example.com",
        "https://search-domain.es.amazonaws.com",  # Missing region
        "https://my-bucket.s3.us-east-1.amazonaws.com",  # S3, not OpenSearch
        "not-a-valid-url",
        "",
    ],
)
def test_detect_aws_opensearch_config_invalid(hostname):
    """Test AWS hostname regex patterns return None for non-AWS hostnames."""
    result = detect_aws_opensearch_config(hostname)
    assert result is None, f"Should not detect AWS config from {hostname}"
