import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.fsspec.gcs import GcsWriteConfig, SimpleGcsConfig


@dataclass
class GcsWriter(Writer):
    connector_config: "SimpleGcsConfig"
    write_config: "GcsWriteConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.fsspec.gcs import GcsDestinationConnector

        return GcsDestinationConnector
