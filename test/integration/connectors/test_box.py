import os
from pathlib import Path

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


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("BOX_APP_CONFIG")
async def test_box_source(tmp_path: Path):
    access_config = BoxAccessConfig(box_app_config=os.environ["BOX_APP_CONFIG"])
    connection_config = BoxConnectionConfig(access_config=access_config)
    indexer_config = BoxIndexerConfig(remote_url="box://utic-test-ingest-fixtures")
    indexer = BoxIndexer(connection_config=connection_config, index_config=indexer_config)

    downloader_config = BoxDownloaderConfig(download_dir=tmp_path)
    downloader = BoxDownloader(
        connection_config=connection_config, download_config=downloader_config
    )

    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="box",
            expected_num_files=2,
        ),
    )
