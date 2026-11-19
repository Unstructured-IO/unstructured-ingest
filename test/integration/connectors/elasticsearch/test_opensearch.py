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
from opensearchpy.exceptions import TransportError

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

    uploader.precheck()
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


@pytest.fixture
def aoss_credentials():
    """Fixture that provides AWS credentials and AOSS host from environment variables."""
    aws_access_key_id = os.getenv("OPENSEARCH_AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("OPENSEARCH_AWS_SECRET_ACCESS_KEY")
    aoss_host = os.getenv("OPENSEARCH_AOSS_HOST")

    if not all([aws_access_key_id, aws_secret_access_key, aoss_host]):
        pytest.skip(
            "AOSS credentials not available. Set OPENSEARCH_AWS_ACCESS_KEY_ID, "
            "OPENSEARCH_AWS_SECRET_ACCESS_KEY, and OPENSEARCH_AOSS_HOST environment variables."
        )

    return {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "aoss_host": aoss_host,
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
        # This extra isolation is required for AWS IAM auth to work correctly
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
    # This extra isolation is required for AWS IAM auth to work correctly
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
        # FIPS-compliant endpoints
        ("https://abc123xyz.us-east-1.aoss-fips.amazonaws.com", "us-east-1", "aoss"),
        ("https://abc123xyz.us-east-2.aoss-fips.amazonaws.com", "us-east-2", "aoss"),
        ("https://abc123xyz.us-gov-west-1.aoss-fips.amazonaws.com", "us-gov-west-1", "aoss"),
        ("https://abc123xyz.ca-central-1.aoss-fips.amazonaws.com", "ca-central-1", "aoss"),
        ("https://search-domain.us-east-1.es-fips.amazonaws.com", "us-east-1", "es"),
        ("https://search-domain.us-east-2.es-fips.amazonaws.com", "us-east-2", "es"),
        ("https://search-domain.us-gov-west-1.es-fips.amazonaws.com", "us-gov-west-1", "es"),
        ("https://search-domain.ca-central-1.es-fips.amazonaws.com", "ca-central-1", "es"),
        # Without https://
        ("search-domain.us-east-1.es.amazonaws.com", "us-east-1", "es"),
        # With port
        ("https://search-domain.us-east-1.es.amazonaws.com:443", "us-east-1", "es"),
        ("https://abc123xyz.us-east-1.aoss-fips.amazonaws.com:443", "us-east-1", "aoss"),
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


# OpenSearch Uploader Configuration Tests
# These tests verify default configuration values for resilience against rate limiting


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
def test_opensearch_uploader_config_batch_size_default():
    """Test that OpenSearchUploaderConfig has a 5MB default batch size.

    This is lower than the Elasticsearch default (15MB) to accommodate
    AWS OpenSearch cluster rate limits and prevent 429 errors.
    """
    config = OpenSearchUploaderConfig(index_name="test_index")
    assert config.batch_size_bytes == 5_000_000, (
        "OpenSearch default batch_size_bytes should be 5MB (5,000,000 bytes)"
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@pytest.mark.parametrize(
    ("status_code", "error_class"),
    [
        (403, "AuthorizationException"),
        (400, "RequestError"),
        (404, "NotFoundError"),
    ],
)
async def test_opensearch_indexer_pit_fallback_to_scroll(status_code, error_class):
    """Test that _get_doc_ids_async falls back to scroll when create_pit fails.

    Covers: 403 (missing permissions), 400 (unsupported endpoint), 404 (pre-2.4 OpenSearch).
    The fallback is scoped to the create_pit call only.
    """
    from unittest.mock import AsyncMock, patch

    from opensearchpy.exceptions import TransportError

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(password="admin"),
        username="admin",
        hosts=["http://localhost:9200"],
        use_ssl=True,
    )
    indexer = OpenSearchIndexer(
        connection_config=connection_config,
        index_config=OpenSearchIndexerConfig(index_name="test_index"),
    )

    expected_ids = {"id1", "id2", "id3"}

    mock_client = AsyncMock()
    mock_client.create_pit = AsyncMock(side_effect=TransportError(status_code, error_class))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("opensearchpy.AsyncOpenSearch", return_value=mock_client),
        patch.object(
            indexer, "_get_doc_ids_scroll", new_callable=AsyncMock, return_value=expected_ids
        ) as mock_scroll,
        patch.object(
            connection_config,
            "get_async_client_kwargs",
            new_callable=AsyncMock,
            return_value={"hosts": ["http://localhost:9200"]},
        ),
    ):
        result = await indexer._get_doc_ids_async()

        mock_client.create_pit.assert_called_once()
        mock_scroll.assert_called_once()
        assert result == expected_ids


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@pytest.mark.parametrize(
    ("exception",),
    [
        (TransportError(500, "internal_server_error"),),
        (ConnectionError("cluster unreachable"),),
    ],
)
async def test_opensearch_indexer_pit_no_fallback_on_other_errors(exception):
    """Test that _get_doc_ids_async re-raises non-fallback errors without trying scroll.

    Covers: 500 (server error), ConnectionError (network). These should NOT fall back.
    """
    from unittest.mock import AsyncMock, patch

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(password="admin"),
        username="admin",
        hosts=["http://localhost:9200"],
        use_ssl=True,
    )
    indexer = OpenSearchIndexer(
        connection_config=connection_config,
        index_config=OpenSearchIndexerConfig(index_name="test_index"),
    )

    mock_client = AsyncMock()
    mock_client.create_pit = AsyncMock(side_effect=exception)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("opensearchpy.AsyncOpenSearch", return_value=mock_client),
        patch.object(indexer, "_get_doc_ids_scroll", new_callable=AsyncMock) as mock_scroll,
        patch.object(
            connection_config,
            "get_async_client_kwargs",
            new_callable=AsyncMock,
            return_value={"hosts": ["http://localhost:9200"]},
        ),
    ):
        with pytest.raises(type(exception)):
            await indexer._get_doc_ids_async()

        mock_client.create_pit.assert_called_once()
        mock_scroll.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
async def test_opensearch_connection_config_retry_settings():
    """Test that OpenSearchConnectionConfig includes retry and timeout settings.

    These settings provide resilience against transient errors (429 rate limiting,
    502/503 server errors) when uploading to OpenSearch clusters.
    """
    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(password="test"),
        username="test",
        hosts=["http://localhost:9200"],
    )

    client_kwargs = await connection_config.get_async_client_kwargs()

    # Verify retry configuration
    assert client_kwargs.get("max_retries") == 3, "Should retry up to 3 times"
    assert client_kwargs.get("retry_on_status") == [429, 502, 503], (
        "Should retry on rate limit (429) and server errors (502, 503)"
    )
    assert client_kwargs.get("retry_on_timeout") is True, "Should retry on timeout"
    assert client_kwargs.get("timeout") == 60, "Should have 60 second timeout"


# AWS OpenSearch Serverless (AOSS) Integration Tests
# These tests require OPENSEARCH_AOSS_HOST in addition to the standard AWS credentials.
# The AOSS collection must have data access policies granting the IAM user
# aoss:ReadDocument and aoss:WriteDocument permissions.


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG, "aws", "aoss")
async def test_opensearch_aoss_source(aoss_credentials: dict):
    """Test OpenSearch source connector against a live AOSS collection.

    Validates the PIT + search_after pagination path, which is required for AOSS
    (scroll is not supported on serverless). Runs indexer and downloader directly
    rather than through source_connector_validation to avoid fixture dependencies.
    """
    indexer_config = OpenSearchIndexerConfig(index_name="opensearch_serverless_e2e_source")

    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        connection_config = OpenSearchConnectionConfig(
            access_config=OpenSearchAccessConfig(
                aws_access_key_id=aoss_credentials["aws_access_key_id"],
                aws_secret_access_key=aoss_credentials["aws_secret_access_key"],
            ),
            hosts=[aoss_credentials["aoss_host"]],
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

        import concurrent.futures

        original_precheck = indexer.precheck

        def threaded_precheck():
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(original_precheck)
                future.result()

        threaded_precheck()

        batch_count = 0
        total_downloaded = 0
        async for file_data in indexer.run_async():
            assert file_data
            batch_count += 1
            resp = await downloader.run_async(file_data=file_data)
            assert resp
            total_downloaded += len(resp)

        assert batch_count == 11, f"Expected 11 batches, got {batch_count}"
        assert total_downloaded == 1010, f"Expected 1010 documents, got {total_downloaded}"


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG, "aws", "aoss")
async def test_opensearch_aoss_destination(
    upload_file: Path,
    tmp_path: Path,
    aoss_credentials: dict,
):
    """Test OpenSearch destination connector against a live AOSS collection.

    Validates upload via async_bulk to serverless. Note: AOSS has eventual
    consistency (10-60s propagation), so we only verify no exceptions are raised.
    """
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data aoss test",
    )

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(
            aws_access_key_id=aoss_credentials["aws_access_key_id"],
            aws_secret_access_key=aoss_credentials["aws_secret_access_key"],
        ),
        hosts=[aoss_credentials["aoss_host"]],
        use_ssl=True,
        verify_certs=True,
    )

    stager = OpenSearchUploadStager(
        upload_stager_config=OpenSearchUploadStagerConfig(
            index_name="opensearch_serverless_e2e_destination"
        )
    )

    uploader = OpenSearchUploader(
        connection_config=connection_config,
        upload_config=OpenSearchUploaderConfig(index_name="opensearch_serverless_e2e_destination"),
    )

    staged_filepath = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )

    import concurrent.futures

    def threaded_precheck():
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(uploader.precheck)
            future.result()

    threaded_precheck()

    await uploader.run_async(path=staged_filepath, file_data=file_data)


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG, "aws", "aoss")
def test_opensearch_aoss_precheck_validates_connection(aoss_credentials: dict):
    """Test that indexer precheck succeeds against a live AOSS collection."""
    indexer_config = OpenSearchIndexerConfig(index_name="opensearch_serverless_e2e_source")

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(
            aws_access_key_id=aoss_credentials["aws_access_key_id"],
            aws_secret_access_key=aoss_credentials["aws_secret_access_key"],
        ),
        hosts=[aoss_credentials["aoss_host"]],
        use_ssl=True,
        verify_certs=True,
    )

    indexer = OpenSearchIndexer(connection_config=connection_config, index_config=indexer_config)
    indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, NOSQL_TAG, "aws", "aoss")
def test_opensearch_aoss_precheck_fail_invalid_credentials():
    """Test that precheck fails with invalid IAM credentials against AOSS."""
    aoss_host = os.getenv("OPENSEARCH_AOSS_HOST")
    if not aoss_host:
        pytest.skip("OPENSEARCH_AOSS_HOST not set")

    indexer_config = OpenSearchIndexerConfig(index_name="opensearch_serverless_e2e_source")

    connection_config = OpenSearchConnectionConfig(
        access_config=OpenSearchAccessConfig(
            aws_access_key_id="INVALID_KEY",
            aws_secret_access_key="INVALID_SECRET",
        ),
        hosts=[aoss_host],
        use_ssl=True,
        verify_certs=True,
    )

    indexer = OpenSearchIndexer(connection_config=connection_config, index_config=indexer_config)

    with pytest.raises(SourceConnectionError):
        indexer.precheck()
