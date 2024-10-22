import random
from pathlib import Path

from unstructured_ingest.v2.interfaces import ProcessorConfig
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.pipeline.pipeline import Pipeline
from unstructured_ingest.v2.processes.chunker import ChunkerConfig
from unstructured_ingest.v2.processes.connectors.local import (
    LocalConnectionConfig,
    LocalDownloaderConfig,
    LocalIndexerConfig,
)
from unstructured_ingest.v2.processes.connectors.vectara import (
    CONNECTOR_TYPE,
    VectaraAccessConfig,
    VectaraConnectionConfig,
    VectaraUploaderConfig,
    VectaraUploadStagerConfig,
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
            access_config=VectaraAccessConfig(uri=None),
            customer_id="2268229652",
            corpus_name=f"test-corpus-vectara-{random.randint(1000,9999)}",
            corpus_id='3232',
            token_url="https://vectara-prod-{}.auth.us-west-2.amazoncognito.com/oauth2/token",
        ),
        stager_config=VectaraUploadStagerConfig(),
        uploader_config=VectaraUploaderConfig(batch_size=10),
    ).run()
