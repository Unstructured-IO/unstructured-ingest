from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.logger import logger
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.chunker import ChunkerConfig
from unstructured_ingest.processes.connectors.fsspec.s3 import (
    CONNECTOR_TYPE,
    S3ConnectionConfig,
    S3DownloaderConfig,
    S3IndexerConfig,
)
from unstructured_ingest.processes.connectors.local import (
    LocalUploaderConfig,
)
from unstructured_ingest.processes.embedder import EmbedderConfig
from unstructured_ingest.processes.filter import FiltererConfig
from unstructured_ingest.processes.partitioner import PartitionerConfig

base_path = Path(__file__).parent.parent.parent.parent
docs_path = base_path / "example-docs"
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"
download_path = work_dir / "download"

if __name__ == "__main__":
    logger.info(f"writing all content in: {work_dir.resolve()}")
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve()), verbose=True, iter_delete=True),
        indexer_config=S3IndexerConfig(remote_url="s3://utic-dev-tech-fixtures/small-pdf-set/"),
        downloader_config=S3DownloaderConfig(download_dir=download_path),
        source_connection_config=S3ConnectionConfig(anonymous=True),
        partitioner_config=PartitionerConfig(strategy="fast"),
        chunker_config=ChunkerConfig(chunking_strategy="by_title"),
        embedder_config=EmbedderConfig(embedding_provider="huggingface"),
        uploader_config=LocalUploaderConfig(output_dir=str(output_path.resolve())),
        filterer_config=FiltererConfig(max_file_size=900000),
    ).run()
