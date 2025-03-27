import os
from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.logger import logger
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.chunker import ChunkerConfig
from unstructured_ingest.processes.connectors.local import (
    LocalConnectionConfig,
    LocalDownloaderConfig,
    LocalIndexerConfig,
)
from unstructured_ingest.processes.connectors.pinecone import (
    CONNECTOR_TYPE,
    PineconeAccessConfig,
    PineconeConnectionConfig,
    PineconeUploaderConfig,
    PineconeUploadStagerConfig,
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
        indexer_config=LocalIndexerConfig(
            input_path=str(docs_path.resolve()) + "/book-war-and-peace-1p.txt"
        ),
        downloader_config=LocalDownloaderConfig(download_dir=download_path),
        source_connection_config=LocalConnectionConfig(),
        partitioner_config=PartitionerConfig(strategy="fast"),
        chunker_config=ChunkerConfig(chunking_strategy="by_title"),
        embedder_config=EmbedderConfig(embedding_provider="huggingface"),
        destination_connection_config=PineconeConnectionConfig(
            # You'll need to set PINECONE_API_KEY environment variable to run this example
            access_config=PineconeAccessConfig(pinecone_api_key=os.getenv("PINECONE_API_KEY")),
            index_name=os.getenv(
                "PINECONE_INDEX",
                default="your index name here. e.g. my-index,"
                "or define in environment variable PINECONE_INDEX",
            ),
            environment=os.getenv(
                "PINECONE_ENVIRONMENT",
                default="your environment name here. e.g. us-east-1,"
                "or define in environment variable PINECONE_ENVIRONMENT",
            ),
        ),
        stager_config=PineconeUploadStagerConfig(),
        uploader_config=PineconeUploaderConfig(batch_size=10, num_processes=2),
    ).run()
