import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseSourceConnector
from unstructured_ingest.logger import logger
from unstructured_ingest.runner.base_runner import Runner
from unstructured_ingest.runner.utils import update_download_dir_remote_url

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.fsspec.gcs import SimpleGcsConfig


@dataclass
class GCSRunner(Runner):
    connector_config: "SimpleGcsConfig"

    def update_read_config(self):
        self.read_config.download_dir = update_download_dir_remote_url(
            connector_name="gcs",
            read_config=self.read_config,
            remote_url=self.connector_config.remote_url,  # type: ignore
            logger=logger,
        )

    def get_source_connector_cls(self) -> t.Type[BaseSourceConnector]:
        from unstructured_ingest.connector.fsspec.gcs import GcsSourceConnector

        return GcsSourceConnector
