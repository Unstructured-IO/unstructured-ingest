import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG, UNCATEGORIZED_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.v2.processes.connectors.discord import (
    CONNECTOR_TYPE,
    DiscordAccessConfig,
    DiscordConnectionConfig,
    DiscordDownloader,
    DiscordDownloaderConfig,
    DiscordIndexer,
    DiscordIndexerConfig,
)


@dataclass(frozen=True)
class EnvData:
    token: Optional[str]
    channels: Optional[list[str]]


def get_env_data() -> EnvData:
    return EnvData(
        token=os.getenv("DISCORD_TOKEN"),
        channels=os.getenv("DISCORD_CHANNELS", default=[]).split(","),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
@requires_env("DISCORD_TOKEN", "DISCORD_CHANNELS")
async def test_discord_source():
    env = get_env_data()
    indexer_config = DiscordIndexerConfig(channels=env.channels)
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        connection_config = DiscordConnectionConfig(
            access_config=DiscordAccessConfig(token=env.token)
        )
        download_config = DiscordDownloaderConfig(download_dir=tempdir_path)
        indexer = DiscordIndexer(connection_config=connection_config, index_config=indexer_config)
        downloader = DiscordDownloader(
            connection_config=connection_config, download_config=download_config
        )
        expected_num_files = len(env.channels)
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=SourceValidationConfigs(
                test_id=CONNECTOR_TYPE,
                expected_num_files=expected_num_files,
                expected_number_indexed_file_data=expected_num_files,
                validate_downloaded_files=True,
            ),
        )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
@requires_env("DISCORD_CHANNELS")
def test_discord_source_precheck_fail_no_token():
    indexer_config = DiscordIndexerConfig(channels=get_env_data().channels)

    connection_config = DiscordConnectionConfig(access_config=DiscordAccessConfig(token=""))
    indexer = DiscordIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
@requires_env("DISCORD_TOKEN")
def test_discord_source_precheck_fail_no_channels():
    indexer_config = DiscordIndexerConfig(channels=[])

    connection_config = DiscordConnectionConfig(
        access_config=DiscordAccessConfig(token=get_env_data().token)
    )
    indexer = DiscordIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()
