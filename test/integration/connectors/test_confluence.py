import os

import pytest

from test.integration.connectors.utils.constants import (
    SOURCE_TAG,
)
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.connectors.confluence import (
    CONNECTOR_TYPE,
    ConfluenceAccessConfig,
    ConfluenceConnectionConfig,
    ConfluenceDownloader,
    ConfluenceDownloaderConfig,
    ConfluenceIndexer,
    ConfluenceIndexerConfig,
)


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@requires_env("CONFLUENCE_USER_EMAIL", "CONFLUENCE_API_TOKEN")
async def test_confluence_source(temp_dir):
    # Retrieve environment variables
    confluence_url = "https://unstructured-ingest-test.atlassian.net"
    user_email = os.environ["CONFLUENCE_USER_EMAIL"]
    api_token = os.environ["CONFLUENCE_API_TOKEN"]
    spaces = ["testteamsp", "MFS"]

    # Create connection and indexer configurations
    access_config = ConfluenceAccessConfig(api_token=api_token)
    connection_config = ConfluenceConnectionConfig(
        url=confluence_url,
        user_email=user_email,
        access_config=access_config,
    )
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=500,
        max_num_of_docs_from_each_space=100,
        spaces=spaces,
    )

    download_config = ConfluenceDownloaderConfig(download_dir=temp_dir)

    # Instantiate indexer and downloader
    indexer = ConfluenceIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = ConfluenceDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=ValidationConfigs(
            test_id="confluence",
            expected_num_files=11,
            validate_downloaded_files=True,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@requires_env("CONFLUENCE_USER_EMAIL", "CONFLUENCE_API_TOKEN")
async def test_confluence_source_large(temp_dir):
    # Retrieve environment variables
    confluence_url = "https://unstructured-ingest-test.atlassian.net"
    user_email = os.environ["CONFLUENCE_USER_EMAIL"]
    api_token = os.environ["CONFLUENCE_API_TOKEN"]
    spaces = ["testteamsp1"]

    # Create connection and indexer configurations
    access_config = ConfluenceAccessConfig(api_token=api_token)
    connection_config = ConfluenceConnectionConfig(
        url=confluence_url,
        user_email=user_email,
        access_config=access_config,
    )
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=10,
        max_num_of_docs_from_each_space=250,
        spaces=spaces,
    )

    download_config = ConfluenceDownloaderConfig(download_dir=temp_dir)

    # Instantiate indexer and downloader
    indexer = ConfluenceIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = ConfluenceDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=ValidationConfigs(
            test_id="confluence_large", expected_num_files=250, validate_file_data=False
        ),
    )
