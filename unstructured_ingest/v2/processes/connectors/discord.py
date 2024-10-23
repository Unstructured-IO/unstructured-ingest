import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
)
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

CONNECTOR_TYPE = "discord"


class DiscordAccessConfig(AccessConfig):
    token: str = Field(description="Discord API token")


class DiscordConnectionConfig(ConnectionConfig):
    access_config: Secret[DiscordAccessConfig]
    channels: Optional[list[str]] = Field(
        default=None, description="List of Discord channel IDs to process"
    )


class DiscordIndexerConfig(IndexerConfig):
    pass


@dataclass
class DiscordIndexer(Indexer):
    connection_config: DiscordConnectionConfig
    index_config: DiscordIndexerConfig

    @requires_dependencies(["discord"], extras="discord")
    def get_client(self):
        import discord

        intents = discord.Intents.default()
        intents.message_content = True
        return discord.Client(intents=intents)

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        client = self.get_client()
        channels_to_process: set[str] = set(self.connection_config.channels or [])

        for channel_id in list(channels_to_process):
            file_data = self.get_channel_file_data(channel_id=channel_id, client=client)
            if file_data:
                yield file_data

    @requires_dependencies(["discord"], extras="discord")
    def get_channel_file_data(self, channel_id: str, client) -> Optional[FileData]:
        # Fetch channel metadata
        dt.datetime.utcnow().isoformat()
        identifier = channel_id
        channel_id = f"{channel_id}.txt"
        source_identifiers = SourceIdentifiers(
            filename=identifier,
            fullpath=channel_id,
            rel_path=identifier,
        )
        metadata = FileDataSourceMetadata(
            record_locator={"channel_id": identifier},
            date_processed=str(dt.datetime.utcnow().isoformat()),
        )
        return FileData(
            identifier=identifier,
            connector_type=CONNECTOR_TYPE,
            source_identifiers=source_identifiers,
            metadata=metadata,
        )


class DiscordDownloaderConfig(DownloaderConfig):
    @requires_dependencies(["discord"], extras="discord")
    def get_client(self):
        import discord

        intents = discord.Intents.default()
        intents.message_content = True
        return discord.Client(intents=intents)


@dataclass
class DiscordDownloader(Downloader):
    connection_config: DiscordConnectionConfig
    download_config: DiscordDownloaderConfig

    @requires_dependencies(["discord"], extras="discord")
    def load_async(self):
        import discord

        return discord.Client, None

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        # Synchronous run is not implemented
        raise NotImplementedError()

    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        import discord
        from discord.ext import commands

        client = self.download_config.get_client()
        record_locator = file_data.metadata.record_locator

        if "channel_id" in record_locator:
            bot = commands.Bot(command_prefix=">", intents=client.intents)
            download_path = self.get_download_path(file_data=file_data)
            download_path.parent.mkdir(parents=True, exist_ok=True)

            messages: list[discord.Message] = []

            @bot.event
            async def on_ready():
                logging.info("Bot is ready")
                record_locator["channel_id"] = record_locator["channel_id"].split(".")[0]
                channel = bot.get_channel(int(record_locator["channel_id"]))
                if channel:
                    logging.info(f"Processing messages for channel: {channel.name}")
                    async for msg in channel.history(limit=100):
                        messages.append(msg)
                    logging.info(f"Fetched {len(messages)} messages")
                else:
                    logging.warning(f"Channel with ID {record_locator['channel_id']} not found")
                await bot.close()

            await bot.start(self.connection_config.access_config.get_secret_value().token)

            with open(download_path, "w") as file:
                for message in messages:
                    file.write(f"{message.content}\n")

            return self.generate_download_response(file_data=file_data, download_path=download_path)
        else:
            raise ValueError("Invalid record_locator in file_data")


discord_source_entry = SourceRegistryEntry(
    indexer=DiscordIndexer,
    indexer_config=DiscordIndexerConfig,
    downloader=DiscordDownloader,
    downloader_config=DiscordDownloaderConfig,
    connection_config=DiscordConnectionConfig,
)
