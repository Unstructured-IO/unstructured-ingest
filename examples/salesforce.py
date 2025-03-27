import os
from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.logger import logger
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.chunker import ChunkerConfig
from unstructured_ingest.processes.connectors.local import (
    LocalUploaderConfig,
)
from unstructured_ingest.processes.connectors.salesforce import (
    CONNECTOR_TYPE,
    SalesforceAccessConfig,
    SalesforceConnectionConfig,
    SalesforceDownloaderConfig,
    SalesforceIndexerConfig,
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
        indexer_config=SalesforceIndexerConfig(categories=["Campaign", "EmailMessage"]),
        downloader_config=SalesforceDownloaderConfig(download_dir=download_path),
        source_connection_config=SalesforceConnectionConfig(
            SalesforceAccessConfig(
                consumer_key=os.getenv("SALESFORCE_CONSUMER_KEY"),
                private_key=os.getenv("SALESFORCE_PRIVATE_KEY"),
            ),
            username=os.getenv("SALESFORCE_USERNAME"),
        ),
        partitioner_config=PartitionerConfig(strategy="fast"),
        chunker_config=ChunkerConfig(chunking_strategy="by_title"),
        embedder_config=EmbedderConfig(embedding_provider="huggingface"),
        uploader_config=LocalUploaderConfig(output_dir=str(output_path.resolve())),
    ).run()
