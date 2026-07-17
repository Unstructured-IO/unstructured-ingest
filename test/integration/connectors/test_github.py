import os

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG, UNCATEGORIZED_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
    source_filedata_display_name_set_check,
)
from test.integration.utils import requires_env
from unstructured_ingest.processes.connectors.github import (
    CONNECTOR_TYPE,
    GithubAccessConfig,
    GithubConnectionConfig,
    GithubDownloader,
    GithubDownloaderConfig,
    GithubIndexer,
    GithubIndexerConfig,
)


async def _validate_github_source(connection_config: GithubConnectionConfig, temp_dir):
    indexer = GithubIndexer(
        connection_config=connection_config,
        index_config=GithubIndexerConfig(file_glob=["*.txt", "*.html"]),
    )

    downloader = GithubDownloader(
        connection_config=connection_config,
        download_config=GithubDownloaderConfig(download_dir=temp_dir),
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="github",
            expected_num_files=2,
            validate_downloaded_files=True,
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
        ),
    )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
@pytest.mark.asyncio
@requires_env("GH_READ_ONLY_ACCESS_TOKEN")
async def test_github_source(temp_dir):
    access_token = os.environ["GH_READ_ONLY_ACCESS_TOKEN"]
    connection_config = GithubConnectionConfig(
        access_config=GithubAccessConfig(access_token=access_token),
        url="dcneiner/Downloadify",
    )
    await _validate_github_source(connection_config, temp_dir)


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
@pytest.mark.asyncio
@requires_env("GH_OAUTH_ACCESS_TOKEN")
async def test_github_source_oauth(temp_dir):
    # The platform's init-oauth-refresh container populates oauth_token with a fresh access
    # token before each run; here we supply an already-valid OAuth token to exercise the path.
    oauth_token = os.environ["GH_OAUTH_ACCESS_TOKEN"]
    connection_config = GithubConnectionConfig(
        access_config=GithubAccessConfig(oauth_token=oauth_token),
        url="dcneiner/Downloadify",
    )
    await _validate_github_source(connection_config, temp_dir)
