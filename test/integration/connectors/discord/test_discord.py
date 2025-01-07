import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pytest
from _pytest.fixtures import TopRequest
import discord
from discord import Client as DiscordClient

from test.integration.connectors.utils.constants import DESTINATION_TAG, SOURCE_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.discord import (
    CONNECTOR_TYPE,
    DiscordAccessConfig,
    DiscordConnectionConfig,
    DiscordDownloader,
    DiscordDownloaderConfig,
    DiscordIndexer,
    DiscordIndexerConfig,
    SourceConnectionError,
)
import os

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
DISCORD_CHANNELS = os.environ["DISCORD_CHANNELS"].split(",")


@contextmanager
def get_client() -> Generator[DiscordClient, None, None]:
    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)
    yield client
    client.close()


@pytest.fixture
def discord_channels() -> list[str]:
    return DISCORD_CHANNELS


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
async def test_discord_source(discord_channels: list[str]):
    indexer_config = DiscordIndexerConfig(channels=discord_channels)
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        connection_config = DiscordConnectionConfig(
            access_config=DiscordAccessConfig(token=DISCORD_TOKEN)
        )
        download_config = DiscordDownloaderConfig(download_dir=tempdir_path)
        indexer = DiscordIndexer(
            connection_config=connection_config, index_config=indexer_config
        )
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
                expected_number_indexed_file_data=1,
                validate_downloaded_files=True,
            ),
        )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
def test_discord_source_precheck_fail_no_token():
    indexer_config = DiscordIndexerConfig(channels=DISCORD_CHANNELS)

    connection_config = DiscordConnectionConfig(
        access_config=DiscordAccessConfig(token="")
    )
    indexer = DiscordIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
def test_discord_source_precheck_fail_no_channels():
    indexer_config = DiscordIndexerConfig(channels=[])

    connection_config = DiscordConnectionConfig(
        access_config=DiscordAccessConfig(token=DISCORD_TOKEN)
    )
    indexer = DiscordIndexer(connection_config=connection_config, index_config=indexer_config)
    with pytest.raises(SourceConnectionError):
        indexer.precheck()

