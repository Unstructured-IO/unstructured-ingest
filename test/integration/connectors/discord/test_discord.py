import os
import tempfile
from pathlib import Path

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
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


@pytest.fixture
def discord_channels() -> list[str]:
    return os.environ["DISCORD_CHANNELS"].split(",")


@pytest.fixture
def discord_token() -> str:
    return os.environ["DISCORD_TOKEN"]


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
async def test_discord_source(discord_token: str, discord_channels: list[str]):
    indexer_config = DiscordIndexerConfig(channels=discord_channels)
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        connection_config = DiscordConnectionConfig(
            access_config=DiscordAccessConfig(token=discord_token)
        )
        download_config = DiscordDownloaderConfig(download_dir=tempdir_path)
        indexer = DiscordIndexer(connection_config=connection_config, index_config=indexer_config)
        downloader = DiscordDownloader(
            connection_config=connection_config, download_config=download_config
        )
        expected_num_files = len(discord_channels)
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


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
def test_discord_source_precheck_fail_no_token(discord_channels: list[str]):
    indexer_config = DiscordIndexerConfig(channels=discord_channels)

    connection_config = DiscordConnectionConfig(access_config=DiscordAccessConfig(token=""))
    indexer = DiscordIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
def test_discord_source_precheck_fail_no_channels(discord_token: str):
    indexer_config = DiscordIndexerConfig(channels=[])

    connection_config = DiscordConnectionConfig(
        access_config=DiscordAccessConfig(token=discord_token)
    )
    indexer = DiscordIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()
