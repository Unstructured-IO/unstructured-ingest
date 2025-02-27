import os
import pytest
from test.integration.connectors.utils.constants import (
    BLOB_STORAGE_TAG,
    SOURCE_TAG,
)
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.connectors.dropbox import (
    CONNECTOR_TYPE as DROPBOX_CONNECTOR_TYPE,
    DropboxAccessConfig,
    DropboxConnectionConfig,
    DropboxDownloader,
    DropboxDownloaderConfig,
    DropboxIndexer,
    DropboxIndexerConfig,
)

@pytest.mark.asyncio
@pytest.mark.tags(DROPBOX_CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("DROPBOX_REFRESH_TOKEN", "DROPBOX_APP_KEY", "DROPBOX_APP_SECRET")
async def test_dropbox_source(temp_dir):
    """
    Integration test for the Dropbox source connector.

    This test indexes data from dropbox://test-input/ and downloads the resulting files,
    then compares them to fixture data. On the first run, set OVERWRITE_FIXTURES=true 
    to generate (or overwrite) the baseline fixture data for future comparisons.
    """
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    app_key = os.getenv("DROPBOX_APP_KEY")
    app_secret = os.getenv("DROPBOX_APP_SECRET")

    # 1) Create the connection config using your Dropbox credentials
    connection_config = DropboxConnectionConfig(
        access_config=DropboxAccessConfig(
            refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )
    )

    # 2) Build the indexer and downloader configs.
    #    For Dropbox, the path can be "test-input" (equivalent to dropbox://test-input/).
    index_config = DropboxIndexerConfig(
        recursive=True,
        path="test-input",  # effectively "dropbox://test-input/"
    )
    downloader_config = DropboxDownloaderConfig(download_dir=temp_dir)

    # 3) Instantiate the indexer and downloader classes
    indexer = DropboxIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = DropboxDownloader(
        connection_config=connection_config,
        download_config=downloader_config,
    )

    # 4) Run the standard source connector validation.
    #    Set overwrite_fixtures=True on the first run to capture initial fixture data.
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="dropbox",
            expected_num_files=1,  # Adjust to match how many files are in test-input
            validate_downloaded_files=True,
            exclude_fields_extend=[
                # Skip fields that vary (e.g., timestamps) in your fixture comparisons.
                "metadata.date_created",
                "metadata.date_modified",
            ],
        ),
        overwrite_fixtures=True,  # or rely on OVERWRITE_FIXTURES env var
    )
