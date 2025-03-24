import os
from pathlib import Path

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG, UNCATEGORIZED_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.errors import UserAuthError
from unstructured_ingest.v2.processes.connectors.zendesk.zendesk import (
    CONNECTOR_TYPE,
    ZendeskAccessConfig,
    ZendeskConnectionConfig,
    ZendeskDownloader,
    ZendeskDownloaderConfig,
    ZendeskIndexer,
    ZendeskIndexerConfig,
)

SUBDOMAIN = "unstructuredhelp"
EMAIL = "test@unstructured.io"


@pytest.mark.asyncio
@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE, UNCATEGORIZED_TAG)
@requires_env("ZENDESK_TOKEN")
async def test_zendesk_source_tickets(temp_dir: Path):
    access_config = ZendeskAccessConfig(api_token=os.environ["ZENDESK_TOKEN"])
    connection_config = ZendeskConnectionConfig(
        subdomain=SUBDOMAIN, email=EMAIL, access_config=access_config
    )

    index_config = ZendeskIndexerConfig(item_type="tickets")

    indexer = ZendeskIndexer(
        connection_config=connection_config,
        index_config=index_config,
        connector_type=CONNECTOR_TYPE,
    )

    # handle downloader.
    download_config = ZendeskDownloaderConfig(download_dir=temp_dir)

    downloader = ZendeskDownloader(
        connection_config=connection_config,
        download_config=download_config,
        connector_type=CONNECTOR_TYPE,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="zendesk-tickets",
            expected_num_files=8,
            validate_file_data=False,
            validate_downloaded_files=True,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE, UNCATEGORIZED_TAG)
@requires_env("ZENDESK_TOKEN")
async def test_zendesk_source_articles(temp_dir):
    access_config = ZendeskAccessConfig(api_token=os.environ["ZENDESK_TOKEN"])
    connection_config = ZendeskConnectionConfig(
        subdomain=SUBDOMAIN, email=EMAIL, access_config=access_config
    )

    index_config = ZendeskIndexerConfig(item_type="articles")

    indexer = ZendeskIndexer(
        connection_config=connection_config,
        index_config=index_config,
        connector_type=CONNECTOR_TYPE,
    )

    # handle downloader.
    download_config = ZendeskDownloaderConfig(download_dir=temp_dir, extract_images=True)

    downloader = ZendeskDownloader(
        connection_config=connection_config,
        download_config=download_config,
        connector_type=CONNECTOR_TYPE,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="zendesk-articles",
            expected_num_files=8,
            validate_file_data=True,
            validate_downloaded_files=True,
        ),
    )


@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE, UNCATEGORIZED_TAG)
def test_zendesk_source_articles_fail(temp_dir):
    access_config = ZendeskAccessConfig(api_token="FAKE_TOKEN")
    connection_config = ZendeskConnectionConfig(
        subdomain=SUBDOMAIN, email=EMAIL, access_config=access_config
    )

    index_config = ZendeskIndexerConfig(item_type="tickets")

    indexer = ZendeskIndexer(
        connection_config=connection_config,
        index_config=index_config,
        connector_type=CONNECTOR_TYPE,
    )
    with pytest.raises(expected_exception=UserAuthError):
        indexer.precheck()
