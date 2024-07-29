import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.elasticsearch import (
        ElasticsearchWriteConfig,
    )
    from unstructured_ingest.connector.opensearch import (
        SimpleOpenSearchConfig,
    )


@dataclass
class OpenSearchWriter(Writer):
    connector_config: "SimpleOpenSearchConfig"
    write_config: "ElasticsearchWriteConfig"

    def get_connector_cls(self) -> BaseDestinationConnector:
        from unstructured_ingest.connector.opensearch import (
            OpenSearchDestinationConnector,
        )

        return OpenSearchDestinationConnector
