import random
from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.chunker import ChunkerConfig
from unstructured_ingest.processes.connectors.local import (
    LocalConnectionConfig,
    LocalDownloaderConfig,
    LocalIndexerConfig,
)
from unstructured_ingest.processes.connectors.mongodb import (
    CONNECTOR_TYPE,
    MongoDBAccessConfig,
    MongoDBConnectionConfig,
    MongoDBUploaderConfig,
    MongoDBUploadStagerConfig,
)
from unstructured_ingest.processes.embedder import EmbedderConfig
from unstructured_ingest.processes.partitioner import PartitionerConfig

base_path = Path(__file__).parent.parent
docs_path = base_path / "example-docs"
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"
download_path = work_dir / "download"

if __name__ == "__main__":
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve()), verbose=True),
        indexer_config=LocalIndexerConfig(input_path=str(docs_path.resolve()) + "/multisimple/"),
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
        destination_connection_config=MongoDBConnectionConfig(
            access_config=MongoDBAccessConfig(uri=None),
            host="localhost",
            port=27017,
            collection=f"test-collection-{random.randint(1000, 9999)}",
            database="testDatabase",
        ),
        stager_config=MongoDBUploadStagerConfig(),
        uploader_config=MongoDBUploaderConfig(batch_size=10),
    ).run()
