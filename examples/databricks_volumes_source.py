import os
from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.chunker import ChunkerConfig
from unstructured_ingest.processes.connectors.databricks.volumes_native import (
    CONNECTOR_TYPE,
    DatabricksNativeVolumesAccessConfig,
    DatabricksNativeVolumesConnectionConfig,
    DatabricksNativeVolumesDownloaderConfig,
    DatabricksNativeVolumesIndexerConfig,
)
from unstructured_ingest.processes.connectors.local import (
    LocalUploaderConfig,
)
from unstructured_ingest.processes.partitioner import PartitionerConfig

base_path = Path(__file__).parent.parent
docs_path = base_path / "example-docs"
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"
download_path = work_dir / "download"

if __name__ == "__main__":
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve()), verbose=True),
        indexer_config=DatabricksNativeVolumesIndexerConfig(
            host=os.environ["DATABRICKS_HOST"],
            catalog=os.environ["DATABRICKS_CATALOG"],
            volume=os.environ["DATABRICKS_VOLUME"],
            volume_path=os.environ["DATABRICKS_VOLUME_PATH"],
        ),
        downloader_config=DatabricksNativeVolumesDownloaderConfig(download_dir=download_path),
        source_connection_config=DatabricksNativeVolumesConnectionConfig(
            access_config=DatabricksNativeVolumesAccessConfig(
                client_id=os.environ["DATABRICKS_CLIENT_ID"],
                client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
            ),
            host=os.environ["DATABRICKS_HOST"],
            catalog=os.environ["DATABRICKS_CATALOG"],
            volume=os.environ["DATABRICKS_VOLUME"],
            volume_path=os.environ["DATABRICKS_VOLUME_PATH"],
        ),
        partitioner_config=PartitionerConfig(strategy="fast"),
        chunker_config=ChunkerConfig(
            chunking_strategy="basic",
        ),
        embedder_config=None,
        uploader_config=LocalUploaderConfig(output_dir=str(output_path.resolve())),
    ).run()
