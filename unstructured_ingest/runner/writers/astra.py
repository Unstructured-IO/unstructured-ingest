import typing as t
from dataclasses import dataclass

from unstructured_ingest.enhanced_dataclass import EnhancedDataClassJsonMixin
from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.astra import AstraWriteConfig, SimpleAstraConfig


@dataclass
class AstraWriter(Writer, EnhancedDataClassJsonMixin):
    write_config: "AstraWriteConfig"
    connector_config: "SimpleAstraConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.astra import (
            AstraDestinationConnector,
        )

        return AstraDestinationConnector
