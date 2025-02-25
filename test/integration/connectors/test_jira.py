import os

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG, UNCATEGORIZED_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.connectors.jira import (
    CONNECTOR_TYPE,
    JiraAccessConfig,
    JiraConnectionConfig,
    JiraDownloader,
    JiraDownloaderConfig,
    JiraIndexer,
    JiraIndexerConfig,
)


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
@requires_env("JIRA_INGEST_USER_EMAIL", "JIRA_INGEST_API_TOKEN")
async def test_jira_source(temp_dir):
    # Retrieve environment variables
    jira_url = os.environ.get(
        "JIRA_INGEST_URL", "https://unstructured-jira-connector-test.atlassian.net"
    )
    user_email = os.environ["JIRA_INGEST_USER_EMAIL"]
    api_token = os.environ["JIRA_INGEST_API_TOKEN"]
    projects = ["JCTP1"]
    boards = ["3"]
    issues = ["JCTP2-1", "JCTP2-2", "JCTP2-3"]

    # Create connection and indexer configurations
    access_config = JiraAccessConfig(password=api_token)
    connection_config = JiraConnectionConfig(
        url=jira_url,
        username=user_email,
        access_config=access_config,
    )
    index_config = JiraIndexerConfig(projects=projects, boards=boards, issues=issues)

    download_config = JiraDownloaderConfig(download_dir=temp_dir)

    # Instantiate indexer and downloader
    indexer = JiraIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = JiraDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="jira",
            expected_num_files=8,
            validate_file_data=True,
            validate_downloaded_files=True,
        ),
    )
