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
@requires_env("JIRA_DC_URL", "JIRA_DC_PAT")
async def test_jira_source(temp_dir):
    # Retrieve environment variables
    jira_url = os.environ.get("JIRA_DC_URL", "http://localhost:8080")  # Default to local Jira DC
    pat_token = os.environ["JIRA_DC_PAT"]

    # Update these values to match your local Jira sample project
    projects = ["sample_project"] 
    boards = ["1"]
    issues = ["SAM-1", "SAM-2"]

    # Create connection and indexer configurations
    access_config = JiraAccessConfig(token=pat_token)
    connection_config = JiraConnectionConfig(
        url=jira_url,
        cloud=False,  # Ensure we're using Jira Data Center
        access_config=access_config,
    )
    index_config = JiraIndexerConfig(projects=projects, boards=boards, issues=issues)

    download_config = JiraDownloaderConfig(download_dir=temp_dir)

    indexer = JiraIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = JiraDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="jira_dc",
            overwrite_fixtures=True,
            expected_num_files=23,
            validate_file_data=True,
            validate_downloaded_files=True,
        ),
    )
