import os
import tempfile
import uuid
from pathlib import Path

import pytest

from test.integration.connectors.utils.constants import (
    BLOB_STORAGE_TAG,
    DESTINATION_TAG,
    SOURCE_TAG,
    env_setup_path,
)
from test.integration.connectors.utils.docker_compose import docker_compose_context
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.errors import UserAuthError, UserError
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.fsspec.s3 import (
    CONNECTOR_TYPE,
    S3AccessConfig,
    S3ConnectionConfig,
    S3Downloader,
    S3DownloaderConfig,
    S3Indexer,
    S3IndexerConfig,
    S3Uploader,
    S3UploaderConfig,
)


def validate_predownload_file_data(file_data: FileData):
    assert file_data.connector_type == CONNECTOR_TYPE
    assert file_data.local_download_path is None


def validate_postdownload_file_data(file_data: FileData):
    assert file_data.connector_type == CONNECTOR_TYPE
    assert file_data.local_download_path is not None


@pytest.fixture
def anon_connection_config() -> S3ConnectionConfig:
    return S3ConnectionConfig(access_config=S3AccessConfig(), anonymous=True)


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
async def test_s3_source(anon_connection_config: S3ConnectionConfig):
    indexer_config = S3IndexerConfig(remote_url="s3://utic-dev-tech-fixtures/small-pdf-set/")
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        download_config = S3DownloaderConfig(download_dir=tempdir_path)
        indexer = S3Indexer(connection_config=anon_connection_config, index_config=indexer_config)
        downloader = S3Downloader(
            connection_config=anon_connection_config, download_config=download_config
        )
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=SourceValidationConfigs(
                test_id="s3",
                predownload_file_data_check=validate_predownload_file_data,
                postdownload_file_data_check=validate_postdownload_file_data,
                expected_num_files=4,
            ),
        )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
async def test_s3_source_special_char(anon_connection_config: S3ConnectionConfig):
    indexer_config = S3IndexerConfig(remote_url="s3://utic-dev-tech-fixtures/special-characters/")
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        download_config = S3DownloaderConfig(download_dir=tempdir_path)
        indexer = S3Indexer(connection_config=anon_connection_config, index_config=indexer_config)
        downloader = S3Downloader(
            connection_config=anon_connection_config, download_config=download_config
        )
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=SourceValidationConfigs(
                test_id="s3-specialchar",
                predownload_file_data_check=validate_predownload_file_data,
                postdownload_file_data_check=validate_postdownload_file_data,
                expected_num_files=1,
            ),
        )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
def test_s3_source_no_access(anon_connection_config: S3ConnectionConfig):
    indexer_config = S3IndexerConfig(remote_url="s3://utic-ingest-test-fixtures/destination/")
    indexer = S3Indexer(connection_config=anon_connection_config, index_config=indexer_config)
    with pytest.raises(UserAuthError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
def test_s3_source_no_bucket(anon_connection_config: S3ConnectionConfig):
    indexer_config = S3IndexerConfig(remote_url="s3://fake-bucket")
    indexer = S3Indexer(connection_config=anon_connection_config, index_config=indexer_config)
    with pytest.raises(UserError):
        indexer.precheck()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, "minio", BLOB_STORAGE_TAG)
async def test_s3_minio_source(anon_connection_config: S3ConnectionConfig):
    anon_connection_config.endpoint_url = "http://localhost:9000"
    indexer_config = S3IndexerConfig(remote_url="s3://utic-dev-tech-fixtures/")
    with docker_compose_context(docker_compose_path=env_setup_path / "minio" / "source"):
        with tempfile.TemporaryDirectory() as tempdir:
            tempdir_path = Path(tempdir)
            download_config = S3DownloaderConfig(download_dir=tempdir_path)
            indexer = S3Indexer(
                connection_config=anon_connection_config, index_config=indexer_config
            )
            downloader = S3Downloader(
                connection_config=anon_connection_config, download_config=download_config
            )
            await source_connector_validation(
                indexer=indexer,
                downloader=downloader,
                configs=SourceValidationConfigs(
                    test_id="s3-minio",
                    predownload_file_data_check=validate_predownload_file_data,
                    postdownload_file_data_check=validate_postdownload_file_data,
                    expected_num_files=1,
                    exclude_fields_extend=[
                        "metadata.date_modified",
                        "metadata.date_created",
                        "additional_metadata.LastModified",
                    ],
                ),
            )


def get_aws_credentials() -> dict:
    access_key = os.getenv("S3_INGEST_TEST_ACCESS_KEY", None)
    assert access_key
    secret_key = os.getenv("S3_INGEST_TEST_SECRET_KEY", None)
    assert secret_key
    return {"aws_access_key_id": access_key, "aws_secret_access_key": secret_key}


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, BLOB_STORAGE_TAG)
@requires_env("S3_INGEST_TEST_ACCESS_KEY", "S3_INGEST_TEST_SECRET_KEY")
async def test_s3_destination(upload_file: Path):
    aws_credentials = get_aws_credentials()
    s3_bucket = "s3://utic-ingest-test-fixtures"
    destination_path = f"{s3_bucket}/destination/{uuid.uuid4()}"
    connection_config = S3ConnectionConfig(
        access_config=S3AccessConfig(
            key=aws_credentials["aws_access_key_id"],
            secret=aws_credentials["aws_secret_access_key"],
        ),
    )
    upload_config = S3UploaderConfig(remote_url=destination_path)
    uploader = S3Uploader(connection_config=connection_config, upload_config=upload_config)
    s3fs = uploader.fs
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    try:
        uploader.precheck()
        if uploader.is_async():
            await uploader.run_async(path=upload_file, file_data=file_data)
        else:
            uploader.run(path=upload_file, file_data=file_data)
        uploaded_files = [
            Path(file) for file in s3fs.ls(path=destination_path) if Path(file).name != "_empty"
        ]
        assert len(uploaded_files) == 1
    finally:
        s3fs.rm(path=destination_path, recursive=True)
