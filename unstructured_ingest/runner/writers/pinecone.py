import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.pinecone import PineconeWriteConfig, SimplePineconeConfig


@dataclass
class PineconeWriter(Writer):
    write_config: "PineconeWriteConfig"
    connector_config: "SimplePineconeConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.pinecone import (
            PineconeDestinationConnector,
        )

        return PineconeDestinationConnector
