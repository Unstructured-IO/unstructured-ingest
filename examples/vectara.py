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
from unstructured_ingest.processes.connectors.vectara import (
    CONNECTOR_TYPE,
    VectaraAccessConfig,
    VectaraConnectionConfig,
    VectaraUploaderConfig,
    VectaraUploadStagerConfig,
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
        destination_connection_config=VectaraConnectionConfig(
            access_config=VectaraAccessConfig(
                oauth_client_id="fill oauth_client_id", oauth_secret="fill oauth_secret"
            ),
            customer_id="fill customer_id",
            corpus_name="fill corpus_name",
            corpus_key="fill corpus_key",
            token_url="fill token_url",
        ),
        stager_config=VectaraUploadStagerConfig(batch_size=10),
        uploader_config=VectaraUploaderConfig(),
    ).run()
