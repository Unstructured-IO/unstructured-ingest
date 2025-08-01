import os
from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.chunker import ChunkerConfig
from unstructured_ingest.processes.connectors.kdbai import (
    CONNECTOR_TYPE,
    KdbaiConnectionConfig,
    KdbaiUploaderConfig,
    KdbaiUploadStagerConfig,
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
input_path = docs_path.resolve() / "pdf" / "fake-memo.pdf"

os.environ["KDBAI_API_KEY"] = "key"
os.environ["KDBAI_ENDPOINT"] = "http://localhost"
os.environ["KDBAI_DATABASE"] = "default"
os.environ["KDBAI_TABLE"] = "table"

if __name__ == "__main__":
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve()), tqdm=True, verbose=True),
        indexer_config=LocalIndexerConfig(
            input_path=docs_path.resolve() / "book-war-and-peace-1p.txt"
        ),
        downloader_config=LocalDownloaderConfig(download_dir=download_path),
        source_connection_config=LocalConnectionConfig(),
        partitioner_config=PartitionerConfig(strategy="fast"),
        chunker_config=ChunkerConfig(chunking_strategy="by_title"),
        embedder_config=EmbedderConfig(embedding_provider="huggingface"),
        destination_connection_config=KdbaiConnectionConfig(
            endpoint=os.environ["KDBAI_ENDPOINT"],
        ),
        stager_config=KdbaiUploadStagerConfig(),
        uploader_config=KdbaiUploaderConfig(
            database_name=os.environ["KDBAI_DATABASE"], table_name=os.environ["KDBAI_TABLE"]
        ),
    ).run()
