import random
from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.logger import logger
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.chunker import ChunkerConfig
from unstructured_ingest.processes.connectors.chroma import (
    CONNECTOR_TYPE,
    ChromaAccessConfig,
    ChromaConnectionConfig,
    ChromaUploaderConfig,
    ChromaUploadStagerConfig,
)
from unstructured_ingest.processes.connectors.local import (
    LocalConnectionConfig,
    LocalDownloaderConfig,
    LocalIndexerConfig,
)
from unstructured_ingest.processes.embedder import EmbedderConfig
from unstructured_ingest.processes.partitioner import PartitionerConfig

base_path = Path(__file__).parent.parent.parent.parent
docs_path = base_path / "example-docs"
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"
download_path = work_dir / "download"

if __name__ == "__main__":
    logger.info(f"writing all content in: {work_dir.resolve()}")
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve())),
        indexer_config=LocalIndexerConfig(input_path=docs_path.resolve() / "multisimple"),
        downloader_config=LocalDownloaderConfig(download_dir=download_path),
        source_connection_config=LocalConnectionConfig(),
        partitioner_config=PartitionerConfig(strategy="fast"),
        chunker_config=ChunkerConfig(
            chunking_strategy="by_title",
            chunk_include_orig_elements=False,
            chunk_max_characters=1500,
            chunk_multipage_sections=True,
        ),
        embedder_config=EmbedderConfig(embedding_provider="huggingface"),
        destination_connection_config=ChromaConnectionConfig(
            access_config=ChromaAccessConfig(settings=None, headers=None),
            host="localhost",
            port=8047,
            collection_name=f"test-collection-{random.randint(1000, 9999)}",
            tenant="default_tenant",
            database="default_database",
        ),
        stager_config=ChromaUploadStagerConfig(),
        uploader_config=ChromaUploaderConfig(batch_size=10),
    ).run()
