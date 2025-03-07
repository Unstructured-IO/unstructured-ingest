import os
from pathlib import Path
from typing import Optional

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG, UNCATEGORIZED_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.errors import UserAuthError
from unstructured_ingest.v2.processes.connectors.zendesk import (
    CONNECTOR_TYPE,
    ZendeskAccessConfig,
    ZendeskConnectionConfig,
    ZendeskDownloader,
    ZendeskDownloaderConfig,
    ZendeskIndexer,
    ZendeskIndexerConfig,
)


async def zendesk_source_test(
    tmp_path: Path,
    token: Optional[str] = None,
    email: Optional[str] = None,
    subdomain: Optional[str] = None,
):

    access_config = ZendeskAccessConfig(api_token=token)
    connection_config = ZendeskConnectionConfig(
        subdomain=subdomain, email=email, access_config=access_config
    )

    index_config = ZendeskIndexerConfig(batch_size=2, item_type="tickets")

    indexer = ZendeskIndexer(
        connection_config=connection_config,
        index_config=index_config,
        connector_type=CONNECTOR_TYPE,
    )

    # handle downloader.
    download_config = ZendeskDownloaderConfig(download_dir=tmp_path)

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
            expected_num_files=4,
            validate_file_data=False,
            validate_downloaded_files=True,
        ),
    )


async def zendesk_source_articles_test(
    tmp_path: Path,
    token: Optional[str] = None,
    email: Optional[str] = None,
    subdomain: Optional[str] = None,
):

    access_config = ZendeskAccessConfig(api_token=token)
    connection_config = ZendeskConnectionConfig(
        subdomain=subdomain, email=email, access_config=access_config
    )

    index_config = ZendeskIndexerConfig(batch_size=2, item_type="articles")

    indexer = ZendeskIndexer(
        connection_config=connection_config,
        index_config=index_config,
        connector_type=CONNECTOR_TYPE,
    )

    # handle downloader.
    download_config = ZendeskDownloaderConfig(download_dir=tmp_path, extract_images=True)

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
            expected_num_files=4,
            validate_file_data=False,
            validate_downloaded_files=True,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE, UNCATEGORIZED_TAG)
@requires_env("ZENDESK_TOKEN")
async def test_zendesk_source(temp_dir):
    await zendesk_source_test(
        tmp_path=temp_dir,
        token=os.environ["ZENDESK_TOKEN"],
        email="test@unstructured.io",
        subdomain="unstructuredhelp",
    )


@pytest.mark.asyncio
@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE, UNCATEGORIZED_TAG)
@requires_env("ZENDESK_TOKEN")
async def test_zendesk_source_articles(temp_dir):
    await zendesk_source_articles_test(
        tmp_path=temp_dir,
        token=os.environ["ZENDESK_TOKEN"],
        email="test@unstructured.io",
        subdomain="unstructuredhelp",
    )


@pytest.mark.asyncio
@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE, UNCATEGORIZED_TAG)
async def test_zendesk_source_articles_fail(temp_dir):
    with pytest.raises(expected_exception=UserAuthError):
        await zendesk_source_articles_test(
            tmp_path=temp_dir,
            token="FORCE_FAIL_TOKEN",
            email="test@unstructured.io",
            subdomain="unstructuredhelp",
        )
