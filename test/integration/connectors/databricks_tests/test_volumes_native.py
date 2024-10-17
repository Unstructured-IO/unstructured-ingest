import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.connectors.databricks.volumes_native import (
    CONNECTOR_TYPE,
    DatabricksNativeVolumesAccessConfig,
    DatabricksNativeVolumesConnectionConfig,
    DatabricksNativeVolumesDownloader,
    DatabricksNativeVolumesDownloaderConfig,
    DatabricksNativeVolumesIndexer,
    DatabricksNativeVolumesIndexerConfig,
)


@dataclass
class EnvData:
    host: str
    client_id: str
    client_secret: str
    catalog: str


def get_env_data() -> EnvData:
    return EnvData(
        host=os.environ["DATABRICKS_HOST"],
        client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
        catalog=os.environ["DATABRICKS_CATALOG"],
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@requires_env(
    "DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "DATABRICKS_CATALOG"
)
async def test_volumes_native_source():
    env_data = get_env_data()
    indexer_config = DatabricksNativeVolumesIndexerConfig(
        recursive=True,
        volume="test-platform",
        volume_path="databricks-volumes-test-input",
        catalog=env_data.catalog,
    )
    connection_config = DatabricksNativeVolumesConnectionConfig(
        host=env_data.host,
        access_config=DatabricksNativeVolumesAccessConfig(
            client_id=env_data.client_id,
            client_secret=env_data.client_secret,
        ),
    )
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        download_config = DatabricksNativeVolumesDownloaderConfig(download_dir=tempdir_path)
        indexer = DatabricksNativeVolumesIndexer(
            connection_config=connection_config, index_config=indexer_config
        )
        downloader = DatabricksNativeVolumesDownloader(
            connection_config=connection_config, download_config=download_config
        )
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=ValidationConfigs(
                test_id="databricks_volumes_native",
                expected_num_files=1,
            ),
        )
