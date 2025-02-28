import os

import pytest
import requests

from test.integration.connectors.utils.constants import (
    BLOB_STORAGE_TAG,
    SOURCE_TAG,
)
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.connectors.fsspec.dropbox import (
    CONNECTOR_TYPE as DROPBOX_CONNECTOR_TYPE,
)
from unstructured_ingest.v2.processes.connectors.fsspec.dropbox import (
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
    then compares them to fixture data.
    """
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    app_key = os.getenv("DROPBOX_APP_KEY")
    app_secret = os.getenv("DROPBOX_APP_SECRET")

    connection_config = DropboxConnectionConfig(
        access_config=DropboxAccessConfig(
            refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )
    )

    index_config = DropboxIndexerConfig(
        recursive=True,
        remote_url="dropbox://test-input",
    )
    downloader_config = DropboxDownloaderConfig(download_dir=temp_dir)

    indexer = DropboxIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = DropboxDownloader(
        connection_config=connection_config,
        download_config=downloader_config,
    )

    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="dropbox",
            expected_num_files=4,
            validate_downloaded_files=True,
            exclude_fields_extend=[
                "metadata.date_created",
                "metadata.date_modified",
            ],
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(DROPBOX_CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("DROPBOX_REFRESH_TOKEN", "DROPBOX_APP_KEY", "DROPBOX_APP_SECRET")
async def test_dropbox_short_lived_token_via_refresh(temp_dir):
    """
    Demonstrates manually generating an access token from refresh credentials,
    then passing ONLY the short-lived token to the Dropbox connector
    (no app_key, app_secret, or refresh_token in the actual connection config).

    This effectively mimics an external system that hands us a short-lived token.
    """
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    app_key = os.getenv("DROPBOX_APP_KEY")
    app_secret = os.getenv("DROPBOX_APP_SECRET")

    #    Manually request a short-lived token from Dropbox's OAuth endpoint
    #    This call is basically what the connector code does internally,
    #    but we're doing it here in the test so we can pass only the short-lived token later.
    response = requests.post(
        "https://api.dropboxapi.com/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        auth=(app_key, app_secret),
        timeout=30,  # seconds
    )
    response.raise_for_status()
    data = response.json()
    short_lived_token = data["access_token"]
    print("Acquired an access token from Dropbox")

    #    Build connection config with ONLY the short-lived token
    #    We omit refresh_token, app_key, and app_secret to confirm that
    #    our connector can operate purely on the short-lived token.
    connection_config = DropboxConnectionConfig(
        access_config=DropboxAccessConfig(
            token=short_lived_token,
            app_key=None,
            app_secret=None,
            refresh_token=None,
        )
    )

    index_config = DropboxIndexerConfig(
        recursive=True,
        remote_url="dropbox://test-input",
    )
    downloader_config = DropboxDownloaderConfig(download_dir=temp_dir)

    indexer = DropboxIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = DropboxDownloader(
        connection_config=connection_config,
        download_config=downloader_config,
    )

    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="dropbox_short_lived_via_refresh",
            expected_num_files=4,
            validate_downloaded_files=True,
            exclude_fields_extend=[
                "metadata.date_created",
                "metadata.date_modified",
            ],
        ),
    )
