import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.sql import SimpleSqlConfig
    from unstructured_ingest.interfaces import WriteConfig


@dataclass
class SqlWriter(Writer):
    write_config: "WriteConfig"
    connector_config: "SimpleSqlConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.sql import (
            SqlDestinationConnector,
        )

        return SqlDestinationConnector
