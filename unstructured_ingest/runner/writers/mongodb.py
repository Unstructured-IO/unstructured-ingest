import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.mongodb import MongoDBWriteConfig, SimpleMongoDBConfig


@dataclass
class MongodbWriter(Writer):
    write_config: "MongoDBWriteConfig"
    connector_config: "SimpleMongoDBConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.mongodb import (
            MongoDBDestinationConnector,
        )

        return MongoDBDestinationConnector
