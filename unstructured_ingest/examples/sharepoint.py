import os
from pathlib import Path

from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.logger import logger
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.connectors.local import (
    LocalUploaderConfig,
)
from unstructured_ingest.processes.connectors.sharepoint import (
    CONNECTOR_TYPE,
    SharepointAccessConfig,
    SharepointConnectionConfig,
    SharepointDownloaderConfig,
    SharepointIndexerConfig,
    SharepointPermissionsConfig,
)
from unstructured_ingest.processes.partitioner import PartitionerConfig

base_path = Path(__file__).parent.parent.parent.parent
docs_path = base_path / "example-docs"
work_dir = base_path / "tmp_ingest" / CONNECTOR_TYPE
output_path = work_dir / "output"
download_path = work_dir / "download"


if __name__ == "__main__":
    logger.info(f"writing all content in: {work_dir.resolve()}")
    Pipeline.from_configs(
        context=ProcessorConfig(work_dir=str(work_dir.resolve()), tqdm=True, verbose=True),
        indexer_config=SharepointIndexerConfig(),
        downloader_config=SharepointDownloaderConfig(download_dir=download_path),
        source_connection_config=SharepointConnectionConfig(
            client_id=os.getenv("SHAREPOINT_CLIENT_ID"),
            site=os.getenv("SHAREPOINT_SITE"),
            access_config=SharepointAccessConfig(client_cred=os.getenv("SHAREPOINT_CRED")),
            permissions_config=SharepointPermissionsConfig(
                permissions_application_id=os.getenv("SHAREPOINT_PERMISSIONS_APP_ID"),
                permissions_client_cred=os.getenv("SHAREPOINT_PERMISSIONS_APP_CRED"),
                permissions_tenant=os.getenv("SHAREPOINT_PERMISSIONS_TENANT"),
            ),
        ),
        partitioner_config=PartitionerConfig(strategy="fast"),
        # chunker_config=ChunkerConfig(chunking_strategy="by_title"),
        # embedder_config=EmbedderConfig(embedding_provider="huggingface"),
        uploader_config=LocalUploaderConfig(output_dir=str(output_path.resolve())),
    ).run()
