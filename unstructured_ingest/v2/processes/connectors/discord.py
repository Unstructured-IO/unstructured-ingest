import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    FileData,
    Indexer,
    IndexerConfig,
    download_responses,
)
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

CONNECTOR_TYPE = "discord"


class DiscordAccessConfig(AccessConfig):
    token: str = Field(description="Discord API token")


class DiscordConnectionConfig(ConnectionConfig):
    access_config: Secret[DiscordAccessConfig]


class DiscordIndexerConfig(IndexerConfig):
    """Connector config for specifying Discord channels and period."""

    channels: Optional[List[str]] = Field(
        default=None, description="List of Discord channel IDs to process"
    )
    # period: Optional[int] = Field(description="Period of the requested channels")


@dataclass
class DiscordIndexer(Indexer):
    """Handles fetching, writing, and cleaning up Discord messages."""

    access_config: DiscordConnectionConfig

    index_config: DiscordIndexerConfig
    registry_name: str = CONNECTOR_TYPE


class DiscordDownloaderConfig(DownloaderConfig):
    pass


class DiscordDownloader(Downloader):

    connector_type: str = CONNECTOR_TYPE
    connection_config: DiscordConnectionConfig
    channel: str
    days: Optional[int] = None

    @property
    def version(self) -> Optional[str]:
        return None

    @property
    def record_locator(self) -> Dict[str, Any]:
        return {"channel": self.channel}

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        """Fetches and writes the messages to a local file."""
        """Fetch messages from the Discord channel."""
        import discord
        from discord.ext import commands

        messages: List[discord.Message] = []
        intents = discord.Intents.default()
        intents.message_content = True
        bot = commands.Bot(command_prefix=">", intents=intents)

        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)

        self.check_connection()

        @bot.event
        async def on_ready():
            try:
                after_date = (
                    dt.datetime.utcnow() - dt.timedelta(days=self.days) if self.days else None
                )
                channel = bot.get_channel(int(self.channel))
                async for msg in channel.history(after=after_date):  # type: ignore
                    messages.append(msg)
                await bot.close()
            except Exception as e:
                logger.error(f"Error fetching messages: {e}")
                await bot.close()
                raise

        bot.run(token=self.connection_config.access_config)
        jump_url = messages[0].jump_url if messages else None

        if not messages:
            raise ValueError(f"No messages retrieved from Discord channel {self.channel}")

        with open(download_path, "w") as file:
            for message in messages:
                file.write(f"{message.content}\n")
                self.generate_download_response(file_data=file_data, download_path=download_path)

    @requires_dependencies(dependencies=["discord"], extras="discord")
    def check_connection(self):
        """Validates the connection to Discord."""
        import discord
        from discord.client import Client

        intents = discord.Intents.default()
        try:
            client = Client(intents=intents)
            client.run(self.connection_config)
        except Exception as e:
            logger.error(f"Failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")


discord_source_entry = SourceRegistryEntry(
    indexer=DiscordIndexer,
    indexer_config=DiscordIndexerConfig,
    downloader=DiscordDownloader,
    downloader_config=DiscordDownloaderConfig,
    connection_config=DiscordConnectionConfig,
)
