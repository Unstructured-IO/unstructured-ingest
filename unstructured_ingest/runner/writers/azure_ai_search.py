import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.azure_ai_search import (
        AzureAISearchWriteConfig,
        SimpleAzureAISearchStorageConfig,
    )


@dataclass
class AzureAiSearchWriter(Writer):
    connector_config: "SimpleAzureAISearchStorageConfig"
    write_config: "AzureAISearchWriteConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.azure_ai_search import (
            AzureAISearchDestinationConnector,
        )

        return AzureAISearchDestinationConnector
