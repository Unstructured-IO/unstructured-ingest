from pathlib import Path

from unstructured_ingest.v2.interfaces import ProcessorConfig
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.pipeline.pipeline import Pipeline
from unstructured_ingest.v2.processes.chunker import ChunkerConfig
from unstructured_ingest.v2.processes.connectors.delta_table import (
    CONNECTOR_TYPE,
    DeltaTableAccessConfig,
    DeltaTableConnectionConfig,
    DeltaTableUploaderConfig,
    DeltaTableUploadStagerConfig,
)
from unstructured_ingest.v2.processes.connectors.local import (
    LocalConnectionConfig,
    LocalDownloaderConfig,
    LocalIndexerConfig,
)
from unstructured_ingest.v2.processes.embedder import EmbedderConfig
from unstructured_ingest.v2.processes.partitioner import PartitionerConfig

base_path = Path(__file__).parent.parent.parent.parent
docs_path = base_path / "example-docs"
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"
download_path = work_dir / "download"

if __name__ == "__main__":
    logger.info(f"writing all content in: {work_dir.resolve()}")
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve())),
        indexer_config=LocalIndexerConfig(
            input_path=str(docs_path.resolve()) + "/book-war-and-peace-1p.txt",
        ),
        downloader_config=LocalDownloaderConfig(download_dir=download_path),
        source_connection_config=LocalConnectionConfig(),
        partitioner_config=PartitionerConfig(strategy="fast"),
        chunker_config=ChunkerConfig(chunking_strategy="by_title"),
        embedder_config=EmbedderConfig(embedding_provider="huggingface"),
        destination_connection_config=DeltaTableConnectionConfig(
            access_config=DeltaTableAccessConfig(), table_uri="example_uri"
        ),
        stager_config=DeltaTableUploadStagerConfig(),
        uploader_config=DeltaTableUploaderConfig(),
    ).run()
