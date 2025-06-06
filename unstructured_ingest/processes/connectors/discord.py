import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import SourceRegistryEntry
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from discord import Client as DiscordClient

CONNECTOR_TYPE = "discord"


class DiscordAccessConfig(AccessConfig):
    token: str = Field(description="Discord API token")


class DiscordConnectionConfig(ConnectionConfig):
    access_config: Secret[DiscordAccessConfig] = Field(
        default=DiscordAccessConfig, validate_default=True
    )

    @requires_dependencies(["discord"], extras="discord")
    def get_client(self) -> "DiscordClient":
        import discord

        intents = discord.Intents.default()
        intents.message_content = True
        return discord.Client(intents=intents)


class DiscordIndexerConfig(IndexerConfig):
    channels: list[str] = Field(
        default=None,
        description="List of Discord channel IDs to process",
    )


@dataclass
class DiscordIndexer(Indexer):
    connection_config: DiscordConnectionConfig
    index_config: DiscordIndexerConfig

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        self.connection_config.get_client()
        channels_to_process: set[str] = set(self.index_config.channels or [])

        for channel_id in list(channels_to_process):
            file_data = self.get_channel_file_data(channel_id=channel_id)
            if file_data:
                yield file_data

    def precheck(self) -> None:
        if not self.connection_config.access_config.get_secret_value().token:
            raise SourceConnectionError("Discord token is missing")
        if not self.index_config.channels:
            raise SourceConnectionError("No channels provided")

    def get_channel_file_data(self, channel_id: str) -> Optional[FileData]:
        # Fetch channel metadata
        identifier = channel_id
        channel_id = f"{channel_id}.txt"
        source_identifiers = SourceIdentifiers(
            filename=channel_id,
            fullpath=channel_id,
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
            display_name=source_identifiers.fullpath,
        )


class DiscordDownloaderConfig(DownloaderConfig):
    limit: Optional[int] = Field(
        default=100, description="Limit on how many messages per channel to pull in"
    )


@dataclass
class DiscordDownloader(Downloader):
    connection_config: DiscordConnectionConfig
    download_config: DiscordDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        return True

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        # Synchronous run is not implemented
        raise NotImplementedError()

    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        record_locator = file_data.metadata.record_locator

        if "channel_id" not in record_locator:
            raise ValueError(f"No channel id in file data record locator: {record_locator}")

        client = self.connection_config.get_client()
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)

        messages = []
        channel_id = record_locator["channel_id"]

        @client.event
        async def on_ready():
            logger.debug("Discord Bot is ready")
            channel = client.get_channel(int(channel_id))
            if not channel:
                raise ValueError(f"channel not found for id: {channel_id}")
            logger.debug(f"Processing messages for channel: {channel.name}")
            async for msg in channel.history(limit=self.download_config.limit):
                messages.append(msg)
            logger.debug(f"Fetched {len(messages)} messages")
            await client.close()

        try:
            await client.start(self.connection_config.access_config.get_secret_value().token)
        finally:
            await client.close()

        content = "\n".join([message.content for message in messages])

        with open(download_path, "w") as file:
            file.write(content)

        return self.generate_download_response(file_data=file_data, download_path=download_path)


discord_source_entry = SourceRegistryEntry(
    indexer=DiscordIndexer,
    indexer_config=DiscordIndexerConfig,
    downloader=DiscordDownloader,
    downloader_config=DiscordDownloaderConfig,
    connection_config=DiscordConnectionConfig,
)
