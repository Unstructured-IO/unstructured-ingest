# test_sharepoint_integration.py
import os

import pytest
from pydantic import SecretStr
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.connectors.sharepoint import (
    CONNECTOR_TYPE,
    SharepointAccessConfig,
    SharepointConnectionConfig,
    SharepointDownloader,
    SharepointDownloaderConfig,
    SharepointIndexer,
    SharepointIndexerConfig,
    SharepointPermissionsConfig,
)

SOURCE_TAG = "source"


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@requires_env(
    "SHAREPOINT_CLIENT_ID", "SHAREPOINT_CRED", "SHAREPOINT_PERMISSIONS_TENANT", "SHAREPOINT_SITE"
)
async def test_sharepoint_source(temp_dir):
    """
    Integration test that:
      1) Creates a SharepointIndexer to list/enumerate items in a given site
      2) Creates a SharepointDownloader to fetch each enumerated item
      3) Runs a validation helper to confirm the end-to-end pipeline
    """
    client_id = os.getenv("SHAREPOINT_CLIENT_ID")
    client_cred = os.getenv("SHAREPOINT_CRED")
    tenant = os.getenv("SHAREPOINT_PERMISSIONS_TENANT")
    site_url = os.getenv("SHAREPOINT_SITE")

    access_config = SharepointAccessConfig(client_cred=client_cred)

    permissions_config = SharepointPermissionsConfig(
        permissions_application_id=client_id,
        permissions_client_cred=SecretStr(client_cred),
        permissions_tenant=tenant,
        authority_url=SecretStr("https://login.microsoftonline.com/"),
    )

    connection_config = SharepointConnectionConfig(
        client_id=client_id,
        site=site_url,
        access_config=access_config,
        permissions_config=permissions_config,
    )

    index_config = SharepointIndexerConfig(
        path=None,
        recursive=True,
        omit_files=False,
        omit_pages=False,
        omit_lists=True,
    )

    download_config = SharepointDownloaderConfig(
        download_dir=temp_dir  # Directory where the files get saved
    )

    indexer = SharepointIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = SharepointDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    expected_files = 7

    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="sharepoint",
            expected_num_files=expected_files,
            validate_downloaded_files=True,
        ),
    )
