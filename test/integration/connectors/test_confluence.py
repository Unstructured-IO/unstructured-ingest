import os

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG, UNCATEGORIZED_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.processes.connectors.confluence import (
    CONNECTOR_TYPE,
    ConfluenceAccessConfig,
    ConfluenceConnectionConfig,
    ConfluenceDownloader,
    ConfluenceDownloaderConfig,
    ConfluenceIndexer,
    ConfluenceIndexerConfig,
)


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
@requires_env("CONFLUENCE_USER_EMAIL", "CONFLUENCE_API_TOKEN")
@pytest.mark.parametrize(
    "spaces,max_num_of_spaces,max_num_of_docs_from_each_space,expected_num_files,validate_downloaded_files,test_id",
    [
        (["testteamsp", "MFS"], 500, 100, 11, True, "confluence"),
        (["testteamsp"], 500, 1, 1, True, "confluence_limit"),
        (["testteamsp1"], 10, 250, 301, False, "confluence_large"),
    ],
)
async def test_confluence_source_param(
    temp_dir,
    spaces,
    max_num_of_spaces,
    max_num_of_docs_from_each_space,
    expected_num_files,
    validate_downloaded_files,
    test_id,
):
    """
    Integration test for the Confluence source connector using various space and document limits.
    """
    confluence_url = "https://unstructured-ingest-test.atlassian.net"
    user_email = os.environ["CONFLUENCE_USER_EMAIL"]
    api_token = os.environ["CONFLUENCE_API_TOKEN"]

    access_config = ConfluenceAccessConfig(api_token=api_token)
    connection_config = ConfluenceConnectionConfig(
        url=confluence_url,
        username=user_email,
        access_config=access_config,
    )
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=max_num_of_spaces,
        max_num_of_docs_from_each_space=max_num_of_docs_from_each_space,
        spaces=spaces,
    )
    download_config = ConfluenceDownloaderConfig(download_dir=temp_dir)

    indexer = ConfluenceIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = ConfluenceDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id=test_id,
            expected_num_files=expected_num_files,
            validate_downloaded_files=validate_downloaded_files,
        ),
    )