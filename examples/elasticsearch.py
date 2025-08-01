import os
from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.chunker import ChunkerConfig
from unstructured_ingest.processes.connectors.elasticsearch import (
    CONNECTOR_TYPE,
    ElasticsearchAccessConfig,
    ElasticsearchConnectionConfig,
    ElasticsearchUploaderConfig,
    ElasticsearchUploadStagerConfig,
)
from unstructured_ingest.processes.connectors.local import (
    LocalConnectionConfig,
    LocalDownloaderConfig,
    LocalIndexerConfig,
)
from unstructured_ingest.processes.embedder import EmbedderConfig
from unstructured_ingest.processes.partitioner import PartitionerConfig

base_path = Path(__file__).parent.parent
docs_path = base_path / "example-docs"
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"
download_path = work_dir / "download"

if __name__ == "__main__":
    index_name = "ingest-test-destination"
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve()), verbose=True),
        indexer_config=LocalIndexerConfig(
            input_path=str(docs_path.resolve()) + "/book-war-and-peace-1p.txt"
        ),
        downloader_config=LocalDownloaderConfig(download_dir=download_path),
        source_connection_config=LocalConnectionConfig(),
        partitioner_config=PartitionerConfig(strategy="fast"),
        chunker_config=ChunkerConfig(chunking_strategy="by_title"),
        embedder_config=EmbedderConfig(embedding_provider="huggingface"),
        destination_connection_config=ElasticsearchConnectionConfig(
            access_config=ElasticsearchAccessConfig(password=os.getenv("ELASTIC_PASSWORD")),
            username=os.getenv("ELASTIC_USERNAME"),
            hosts=["http://localhost:9200"],
        ),
        uploader_config=ElasticsearchUploaderConfig(index_name=index_name),
        stager_config=ElasticsearchUploadStagerConfig(index_name=index_name),
    ).run()
