import os
from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.logger import logger
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.connectors.discord import (
    CONNECTOR_TYPE,
    DiscordAccessConfig,
    DiscordConnectionConfig,
    DiscordDownloaderConfig,
    DiscordIndexerConfig,
)
from unstructured_ingest.processes.connectors.local import LocalUploaderConfig
from unstructured_ingest.processes.partitioner import PartitionerConfig

base_path = Path(__file__).parent.parent.parent.parent
docs_path = base_path / "example-docs"
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"
download_path = work_dir / "download"

if __name__ == "__main__":
    logger.info(f"writing all content in: {work_dir.resolve()}")
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve()), tqdm=True, verbose=True),
        indexer_config=DiscordIndexerConfig(channels=os.environ["DISCORD_CHANNELS"].split(",")),
        downloader_config=DiscordDownloaderConfig(limit=int(os.getenv("DISCORD_LIMIT", 100))),
        source_connection_config=DiscordConnectionConfig(
            access_config=DiscordAccessConfig(token=os.environ["DISCORD_TOKEN"])
        ),
        partitioner_config=PartitionerConfig(strategy="fast"),
        # chunker_config=ChunkerConfig(chunking_strategy="by_title"),
        # embedder_config=EmbedderConfig(embedding_provider="huggingface"),
        uploader_config=LocalUploaderConfig(output_dir=str(output_path.resolve())),
    ).run()
