import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.azure_cognitive_search import (
        AzureCognitiveSearchWriteConfig,
        SimpleAzureCognitiveSearchStorageConfig,
    )


@dataclass
class AzureCognitiveSearchWriter(Writer):
    connector_config: "SimpleAzureCognitiveSearchStorageConfig"
    write_config: "AzureCognitiveSearchWriteConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.azure_cognitive_search import (
            AzureCognitiveSearchDestinationConnector,
        )

        return AzureCognitiveSearchDestinationConnector
