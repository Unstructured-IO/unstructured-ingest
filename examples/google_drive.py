import os
from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.chunker import ChunkerConfig
from unstructured_ingest.processes.connectors.google_drive import (
    CONNECTOR_TYPE,
    GoogleDriveAccessConfig,
    GoogleDriveConnectionConfig,
    GoogleDriveDownloaderConfig,
    GoogleDriveIndexerConfig,
)
from unstructured_ingest.processes.connectors.local import (
    LocalUploaderConfig,
)
from unstructured_ingest.processes.partitioner import PartitionerConfig

base_path = Path(__file__).parent.parent.parent.parent
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"


if __name__ == "__main__":
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve())),
        # You'll need to set GOOGLE_DRIVE_SERVICE_KEY and GOOGLE_DRIVE_DRIVE_ID
        # environment variable to run this example
        source_connection_config=GoogleDriveConnectionConfig(
            access_config=GoogleDriveAccessConfig(
                service_account_key=os.environ.get("GOOGLE_DRIVE_SERVICE_KEY")
            ),
            drive_id=os.environ.get("GOOGLE_DRIVE_DRIVE_ID"),
        ),
        indexer_config=GoogleDriveIndexerConfig(
            resursive=True,
        ),
        downloader_config=GoogleDriveDownloaderConfig(),
        partitioner_config=PartitionerConfig(strategy="fast"),
        chunker_config=ChunkerConfig(
            chunking_strategy="basic",
        ),
        embedder_config=None,
        uploader_config=LocalUploaderConfig(output_dir=output_path),
    ).run()
