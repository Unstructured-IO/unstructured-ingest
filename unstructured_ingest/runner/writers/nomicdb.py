import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.nomic import NomicWriteConfig, SimpleNomicConfig


@dataclass
class NomicWriter(Writer):
    write_config: "NomicWriteConfig"
    connector_config: "SimpleNomicConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.nomicdb import NomicDestinationConnector

        return NomicDestinationConnector
