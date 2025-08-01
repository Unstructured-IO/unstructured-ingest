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
    source_filedata_display_name_set_check,
)
from test.integration.utils import requires_env
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.errors_v2 import UserAuthError, UserError
from unstructured_ingest.processes.connectors.fsspec.s3 import (
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
                predownload_file_data_check=(
                    validate_predownload_file_data,
                    source_filedata_display_name_set_check,
                ),
                postdownload_file_data_check=(
                    validate_postdownload_file_data,
                    source_filedata_display_name_set_check,
                ),
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
                predownload_file_data_check=(
                    validate_predownload_file_data,
                    source_filedata_display_name_set_check,
                ),
                postdownload_file_data_check=(
                    validate_postdownload_file_data,
                    source_filedata_display_name_set_check,
                ),
                expected_num_files=2,
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
    with (
        docker_compose_context(docker_compose_path=env_setup_path / "minio" / "source"),
        tempfile.TemporaryDirectory() as tempdir,
    ):
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
                test_id="s3-minio",
                predownload_file_data_check=(
                    validate_predownload_file_data,
                    source_filedata_display_name_set_check,
                ),
                postdownload_file_data_check=(
                    validate_postdownload_file_data,
                    source_filedata_display_name_set_check,
                ),
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


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, BLOB_STORAGE_TAG)
@requires_env("S3_INGEST_TEST_ACCESS_KEY", "S3_INGEST_TEST_SECRET_KEY")
async def test_s3_destination_same_filename_different_folders(upload_file: Path):
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
    file_data_1 = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath="folder1/" + upload_file.name, filename=upload_file.name
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    file_data_2 = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath="folder2/" + upload_file.name, filename=upload_file.name
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )

    try:
        uploader.precheck()
        if uploader.is_async():
            await uploader.run_async(path=upload_file, file_data=file_data_1)
            await uploader.run_async(path=upload_file, file_data=file_data_2)
        else:
            uploader.run(path=upload_file, file_data=file_data_1)
            uploader.run(path=upload_file, file_data=file_data_2)
        uploaded_files = [
            Path(file) for file in s3fs.ls(path=destination_path) if Path(file).name != "_empty"
        ]
        assert len(uploaded_files) == 2
    finally:
        s3fs.rm(path=destination_path, recursive=True)


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, BLOB_STORAGE_TAG)
@requires_env("S3_INGEST_TEST_ACCESS_KEY", "S3_INGEST_TEST_SECRET_KEY")
async def test_s3_destination_different_relative_path_and_full_path(upload_file: Path):
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
        source_identifiers=SourceIdentifiers(
            relative_path=f"folder2/{upload_file.name}",
            fullpath=f"folder1/folder2/{upload_file.name}",
            filename=upload_file.name,
        ),
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
        assert uploaded_files[0].as_posix() == f"{destination_path.lstrip('s3://')}/folder1"
    finally:
        s3fs.rm(path=destination_path, recursive=True)


class TestS3SecurityFeatures:
    """Test suite for S3 security features and ambient credentials"""

    def test_ambient_credentials_field_default(self):
        """Test that ambient_credentials defaults to False"""
        access_config = S3AccessConfig()
        assert access_config.ambient_credentials is False

    def test_ambient_credentials_field_explicit(self):
        """Test setting ambient_credentials explicitly"""
        access_config = S3AccessConfig(ambient_credentials=True)
        assert access_config.ambient_credentials is True

    def test_default_security_blocks_automatic_credentials(self):
        """Test that default behavior blocks automatic credential pickup"""
        # No explicit credentials provided, anonymous=False (default)
        access_config = S3AccessConfig()
        connection_config = S3ConnectionConfig(access_config=access_config, anonymous=False)

        # Should raise UserAuthError instead of silently changing behavior
        with pytest.raises(UserAuthError, match="No authentication method specified"):
            connection_config.get_access_config()

    def test_explicit_credentials_work_normally(self):
        """Test that explicit credentials bypass security checks"""
        access_config = S3AccessConfig(key="test-key", secret="test-secret")
        connection_config = S3ConnectionConfig(access_config=access_config, anonymous=False)

        config = connection_config.get_access_config()

        # Should use explicit credentials
        assert config["anon"] is False
        assert config["key"] == "test-key"
        assert config["secret"] == "test-secret"

    def test_explicit_anonymous_mode_respected(self):
        """Test that explicit anonymous=True is respected"""
        access_config = S3AccessConfig()
        connection_config = S3ConnectionConfig(access_config=access_config, anonymous=True)

        config = connection_config.get_access_config()

        # Should be anonymous
        assert config["anon"] is True
        assert "key" not in config

    def test_ambient_credentials_requires_env_var(self, monkeypatch):
        """Test that ambient_credentials=True requires ALLOW_AMBIENT_CREDENTIALS env var"""
        # Clear the environment variable
        monkeypatch.delenv("ALLOW_AMBIENT_CREDENTIALS", raising=False)

        access_config = S3AccessConfig(ambient_credentials=True)
        connection_config = S3ConnectionConfig(access_config=access_config, anonymous=False)

        # Should raise error when env var is not set
        with pytest.raises(
            UserAuthError, match="ALLOW_AMBIENT_CREDENTIALS environment variable is not set"
        ):
            connection_config.get_access_config()

    def test_ambient_credentials_enables_ambient_mode(self, monkeypatch):
        """Test that ambient_credentials=True enables ambient credential pickup
        when env var is set"""
        # Set the environment variable
        monkeypatch.setenv("ALLOW_AMBIENT_CREDENTIALS", "true")

        access_config = S3AccessConfig(ambient_credentials=True)
        connection_config = S3ConnectionConfig(access_config=access_config, anonymous=False)

        config = connection_config.get_access_config()

        # Should allow ambient credentials (anon=False, no explicit credentials)
        assert config["anon"] is False
        assert "key" not in config
        assert "secret" not in config
        assert "token" not in config

    def test_ambient_credentials_field_excluded_from_config(self):
        """Test that ambient_credentials field is not passed to s3fs"""
        # Test with explicit credentials
        access_config = S3AccessConfig(
            key="test-key",
            secret="test-secret",
            ambient_credentials=True,  # Should be excluded
        )
        connection_config = S3ConnectionConfig(access_config=access_config)

        config = connection_config.get_access_config()

        # ambient_credentials should not appear in final config
        assert "ambient_credentials" not in config
        assert config["key"] == "test-key"
        assert config["secret"] == "test-secret"

    def test_none_values_filtered_but_falsy_values_preserved(self):
        """Test that None values are filtered but other falsy values are preserved"""
        access_config = S3AccessConfig(
            key="test-key",
            secret=None,  # Should be filtered
            token="",  # Should be preserved (empty string)
        )
        connection_config = S3ConnectionConfig(access_config=access_config)

        config = connection_config.get_access_config()

        # None should be filtered, empty string should be preserved
        assert config["key"] == "test-key"
        assert "secret" not in config  # None was filtered
        assert config["token"] == ""  # Empty string preserved

    def test_endpoint_url_preserved_with_all_auth_modes(self, monkeypatch):
        """Test that endpoint_url is preserved across all authentication modes"""
        endpoint = "https://custom-s3.example.com"

        # Test with explicit credentials
        access_config = S3AccessConfig(key="test-key", secret="test-secret")
        connection_config = S3ConnectionConfig(access_config=access_config, endpoint_url=endpoint)
        config = connection_config.get_access_config()
        assert config["endpoint_url"] == endpoint

        # Test with ambient credentials
        monkeypatch.setenv("ALLOW_AMBIENT_CREDENTIALS", "true")
        access_config = S3AccessConfig(ambient_credentials=True)
        connection_config = S3ConnectionConfig(access_config=access_config, endpoint_url=endpoint)
        config = connection_config.get_access_config()
        assert config["endpoint_url"] == endpoint

        # Test with anonymous mode
        connection_config = S3ConnectionConfig(anonymous=True, endpoint_url=endpoint)
        config = connection_config.get_access_config()
        assert config["endpoint_url"] == endpoint

    def test_security_error_raised(self):
        """Test that security error is raised when automatic credentials would be used"""
        access_config = S3AccessConfig(ambient_credentials=False)
        connection_config = S3ConnectionConfig(access_config=access_config, anonymous=False)

        # This should raise UserAuthError with helpful message
        with pytest.raises(UserAuthError) as exc_info:
            connection_config.get_access_config()

        # Should provide clear error message
        error_message = str(exc_info.value)
        assert "No authentication method specified" in error_message
        assert "ambient_credentials=False" in error_message

    def test_ambient_credentials_env_var_variations(self, monkeypatch):
        """Test that only 'true' (case-insensitive) values for ALLOW_AMBIENT_CREDENTIALS work"""
        valid_values = ["true", "TRUE", "True", "tRuE"]

        access_config = S3AccessConfig(ambient_credentials=True)
        connection_config = S3ConnectionConfig(access_config=access_config, anonymous=False)

        for value in valid_values:
            monkeypatch.setenv("ALLOW_AMBIENT_CREDENTIALS", value)

            # Should not raise error
            config = connection_config.get_access_config()
            assert config["anon"] is False

    def test_ambient_credentials_info_logged(self, caplog, monkeypatch):
        """Test that info message is logged when using ambient credentials"""
        import logging

        # Set the environment variable
        monkeypatch.setenv("ALLOW_AMBIENT_CREDENTIALS", "true")

        # Ensure we capture INFO level logs
        caplog.set_level(logging.INFO)

        access_config = S3AccessConfig(ambient_credentials=True)
        connection_config = S3ConnectionConfig(access_config=access_config, anonymous=False)

        # This should trigger the ambient credentials info log
        config = connection_config.get_access_config()

        # Should use ambient credentials
        assert config["anon"] is False

        # Should log ambient credentials info
        assert "Using ambient AWS credentials" in caplog.text
