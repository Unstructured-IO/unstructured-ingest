import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.qdrant import QdrantWriteConfig, SimpleQdrantConfig


@dataclass
class QdrantWriter(Writer):
    write_config: "QdrantWriteConfig"
    connector_config: "SimpleQdrantConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.qdrant import QdrantDestinationConnector

        return QdrantDestinationConnector
