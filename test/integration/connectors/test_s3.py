import tempfile
from pathlib import Path

import pytest

from unstructured_ingest.v2.interfaces import FileData
from unstructured_ingest.v2.processes.connectors.fsspec.s3 import (
    CONNECTOR_TYPE,
    S3AccessConfig,
    S3ConnectionConfig,
    S3Downloader,
    S3DownloaderConfig,
    S3Indexer,
    S3IndexerConfig,
)


def validate_predownload_file_data(file_data: FileData):
    assert file_data.connector_type == CONNECTOR_TYPE
    assert file_data.local_download_path is None


def validate_postdownload_file_data(file_data: FileData):
    assert file_data.connector_type == CONNECTOR_TYPE
    assert file_data.local_download_path is not None


@pytest.fixture
def connection_config() -> S3ConnectionConfig:
    return S3ConnectionConfig(access_config=S3AccessConfig(), anonymous=True)


@pytest.mark.asyncio
async def test_s3_source(connection_config: S3ConnectionConfig):
    indexer_config = S3IndexerConfig(remote_url="s3://utic-dev-tech-fixtures/small-pdf-set/")
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        download_config = S3DownloaderConfig(download_dir=tempdir_path)
        indexer = S3Indexer(connection_config=connection_config, index_config=indexer_config)
        downloader = S3Downloader(
            connection_config=connection_config, download_config=download_config
        )
        for file_data in indexer.run():
            assert file_data
            validate_predownload_file_data(file_data=file_data)
            if downloader.is_async():
                resp = await downloader.run_async(file_data=file_data)
            else:
                resp = downloader.run(file_data=file_data)
            postdownload_file_data = resp["file_data"]
            validate_postdownload_file_data(file_data=postdownload_file_data)
        downloaded_files = [p for p in tempdir_path.rglob("*") if p.is_file()]
        assert len(downloaded_files) == 4
