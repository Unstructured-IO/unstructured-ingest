import os

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG, UNCATEGORIZED_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.connectors.github import (
    CONNECTOR_TYPE,
    GithubAccessConfig,
    GithubConnectionConfig,
    GithubDownloader,
    GithubDownloaderConfig,
    GithubIndexer,
    GithubIndexerConfig,
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
            test_id="github", expected_num_files=2, validate_downloaded_files=True
        ),
    )
