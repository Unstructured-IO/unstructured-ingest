import os
import sqlite3
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
from unstructured_ingest.v2.processes.connectors.sql import (
    CONNECTOR_TYPE,
    POSTGRESQL_DB,
    SQLITE_DB,
    SQLAccessConfig,
    SQLConnectionConfig,
    SQLUploaderConfig,
    SQLUploadStagerConfig,
)
from unstructured_ingest.v2.processes.embedder import EmbedderConfig
from unstructured_ingest.v2.processes.partitioner import PartitionerConfig

base_path = Path(__file__).parent.parent.parent.parent
docs_path = base_path / "example-docs"
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"
download_path = work_dir / "download"

SQLITE_DB_PATH = "test-sql-db.sqlite"

if __name__ == "__main__":
    logger.info(f"writing all content in: {work_dir.resolve()}")

    configs = {
        "context": ProcessorConfig(work_dir=str(work_dir.resolve())),
        "indexer_config": LocalIndexerConfig(input_path=str(docs_path.resolve()) + "/multisimple/"),
        "downloader_config": LocalDownloaderConfig(download_dir=download_path),
        "source_connection_config": LocalConnectionConfig(),
        "partitioner_config": PartitionerConfig(strategy="fast"),
        "chunker_config": ChunkerConfig(
            chunking_strategy="by_title",
            chunk_include_orig_elements=False,
            chunk_max_characters=1500,
            chunk_multipage_sections=True,
        ),
        "embedder_config": EmbedderConfig(embedding_provider="huggingface"),
        "stager_config": SQLUploadStagerConfig(),
        "uploader_config": SQLUploaderConfig(batch_size=10),
    }

    if os.path.exists(SQLITE_DB):
        os.remove(SQLITE_DB)

    connection = sqlite3.connect(database=SQLITE_DB)

    query = None
    script_path = (
        Path(__file__).parent.parent.parent.parent.parent
        / Path("test_e2e/env_setup/sql/sqlite-schema.sql")
    ).resolve()
    with open(script_path) as f:
        query = f.read()
    cursor = connection.cursor()
    cursor.executescript(query)
    connection.close()

    # sqlite test first
    Pipeline.from_configs(
        destination_connection_config=SQLConnectionConfig(
            db_type=SQLITE_DB,
            database=SQLITE_DB_PATH,
            access_config=SQLAccessConfig(),
        ),
        **configs,
    ).run()

    # now, pg with pgvector
    Pipeline.from_configs(
        destination_connection_config=SQLConnectionConfig(
            db_type=POSTGRESQL_DB,
            database="elements",
            host="localhost",
            port=5433,
            access_config=SQLAccessConfig(username="unstructured", password="test"),
        ),
        **configs,
    ).run()
