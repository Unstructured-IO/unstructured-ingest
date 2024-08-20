import typing as t
from dataclasses import dataclass

from unstructured_ingest.enhanced_dataclass import EnhancedDataClassJsonMixin
from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.astradb import AstraDBWriteConfig, SimpleAstraDBConfig


@dataclass
class AstraDBWriter(Writer, EnhancedDataClassJsonMixin):
    write_config: "AstraDBWriteConfig"
    connector_config: "SimpleAstraDBConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.astradb import (
            AstraDBDestinationConnector,
        )

        return AstraDBDestinationConnector
