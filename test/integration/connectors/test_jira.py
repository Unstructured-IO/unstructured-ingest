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
@requires_env("JIRA_USER_EMAIL", "JIRA_API_TOKEN")
async def test_jira_source_all_projects(temp_dir):
    # Retrieve environment variables
    # jira_url="https://unstructured-jira-connector-test.atlassian.net/"
    # jira_url = "https://unstructured-ingest-test.atlassian.net"
    jira_url = os.environ["JIRA_URL"]
    user_email = os.environ["JIRA_USER_EMAIL"]
    api_token = os.environ["JIRA_API_TOKEN"]

    # Create connection and indexer configurations
    access_config = JiraAccessConfig(password=api_token)
    connection_config = JiraConnectionConfig(
        url=jira_url,
        username=user_email,
        access_config=access_config,
    )
    index_config = JiraIndexerConfig()

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
            test_id="jira_all_projects",
            expected_num_files=16,
            validate_downloaded_files=True,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
@requires_env("JIRA_USER_EMAIL", "JIRA_API_TOKEN")
async def test_jira_source_boards_and_issues(temp_dir):
    # Retrieve environment variables
    jira_url = os.environ["JIRA_URL"]
    user_email = os.environ["JIRA_USER_EMAIL"]
    api_token = os.environ["JIRA_API_TOKEN"]
    boards = ["2"]
    issues = ["JCTP1-1", "JCTP1-2", "JCTP3-1"]

    # Create connection and indexer configurations
    access_config = JiraAccessConfig(password=api_token)
    connection_config = JiraConnectionConfig(
        url=jira_url,
        username=user_email,
        access_config=access_config,
    )
    index_config = JiraIndexerConfig(boards=boards, issues=issues)

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
            test_id="jira_projects_boards_issues",
            expected_num_files=14,
            validate_file_data=True,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
@requires_env("JIRA_USER_EMAIL", "JIRA_API_TOKEN")
async def test_jira_source_all(temp_dir):
    # Retrieve environment variables
    jira_url = os.environ["JIRA_URL"]
    user_email = os.environ["JIRA_USER_EMAIL"]
    api_token = os.environ["JIRA_API_TOKEN"]
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
            test_id="jira_projects_boards_issues",
            expected_num_files=8,
            validate_file_data=True,
        ),
    )
