import os
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
from unstructured_ingest.v2.processes.connectors.sql.databricks_delta_tables import (
    CONNECTOR_TYPE,
    DatabrickDeltaTablesAccessConfig,
    DatabrickDeltaTablesConnectionConfig,
    DatabrickDeltaTablesUploaderConfig,
    DatabrickDeltaTablesUploadStagerConfig,
)
from unstructured_ingest.v2.processes.embedder import EmbedderConfig
from unstructured_ingest.v2.processes.partitioner import PartitionerConfig

# work_dir = Path("/path/to/work_dir")
# docs_path = Path("/path/to/docs_path")
# download_path = work_dir / "download"
base_path = Path(__file__).parent.parent.parent.parent
docs_path = base_path / "example-docs"
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"
download_path = work_dir / "download"

if __name__ == "__main__":
    logger.info(f"writing all content in: {work_dir.resolve()}")
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve())),
        indexer_config=LocalIndexerConfig(input_path=str(docs_path.resolve()) + "/pdf/DA-1p.pdf"),
        downloader_config=LocalDownloaderConfig(download_dir=download_path),
        source_connection_config=LocalConnectionConfig(),
        partitioner_config=PartitionerConfig(strategy="fast"),
        chunker_config=ChunkerConfig(
            chunking_strategy="by_title",
            chunk_include_orig_elements=False,
            chunk_max_characters=1500,
            chunk_multipage_sections=True,
        ),
        # embedder_config=None,
        embedder_config=EmbedderConfig(embedding_provider="huggingface"),
        destination_connection_config=DatabrickDeltaTablesConnectionConfig(
            access_config=DatabrickDeltaTablesAccessConfig(
                token=os.getenv("DATABRICKS_TOKEN"),
                client_id=os.getenv("DATABRICKS_CLIENT_ID"),
                client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
            ),
            server_hostname="dbc-bc1d7077-003f.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/468764b19449c18a",
        ),
        stager_config=DatabrickDeltaTablesUploadStagerConfig(),
        uploader_config=DatabrickDeltaTablesUploaderConfig(
            catalog="utic-dev-tech-fixtures",
            database="default",
            table="deleteme_elements",
        ),
    ).run()
