import os

import pytest

from test.integration.connectors.utils.constants import BLOB_STORAGE_TAG, SOURCE_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.processes.connectors.fsspec.box import (
    CONNECTOR_TYPE,
    BoxAccessConfig,
    BoxConnectionConfig,
    BoxDownloader,
    BoxDownloaderConfig,
    BoxIndexer,
    BoxIndexerConfig,
)


def make_box_components(remote_url: str, download_dir):
    app_config = os.environ["BOX_APP_CONFIG"]
    connection_config = BoxConnectionConfig(
        access_config=BoxAccessConfig(box_app_config=app_config)
    )
    index_config = BoxIndexerConfig(remote_url=remote_url)
    download_config = BoxDownloaderConfig(download_dir=download_dir)
    indexer = BoxIndexer(connection_config=connection_config, index_config=index_config)
    downloader = BoxDownloader(connection_config=connection_config, download_config=download_config)
    return indexer, downloader


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("BOX_APP_CONFIG")
async def test_box_top_folder(temp_dir):
    """
    Integration test for Box source connector against the top-level ACL test folder.
    Validates that permissions_data is populated from direct folder collaborations.
    """
    indexer, downloader = make_box_components(
        remote_url="box://TestACLs-topfolder",
        download_dir=temp_dir,
    )
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="box_top_folder",
            validate_downloaded_files=False,
            validate_file_data=True,
            exclude_fields_extend=[
                "metadata.date_processed",
            ],
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("BOX_APP_CONFIG")
async def test_box_second_tier(temp_dir):
    """
    Integration test for Box source connector against the nested ACL test folder.
    Validates that permissions_data reflects inherited permissions from the parent folder.
    """
    indexer, downloader = make_box_components(
        remote_url="box://TestACLs-topfolder/TestACLs-secondtier",
        download_dir=temp_dir,
    )
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="box_second_tier",
            validate_downloaded_files=False,
            validate_file_data=True,
            exclude_fields_extend=[
                "metadata.date_processed",
            ],
        ),
    )
