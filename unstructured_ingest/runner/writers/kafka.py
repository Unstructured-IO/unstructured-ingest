import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.kafka import KafkaWriteConfig, SimpleKafkaConfig


@dataclass
class KafkaWriter(Writer):
    write_config: "KafkaWriteConfig"
    connector_config: "SimpleKafkaConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.kafka import (
            KafkaDestinationConnector,
        )

        return KafkaDestinationConnector
