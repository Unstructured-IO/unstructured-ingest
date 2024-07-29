import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.fsspec.box import BoxWriteConfig, SimpleBoxConfig


@dataclass
class BoxWriter(Writer):
    connector_config: "SimpleBoxConfig"
    write_config: "BoxWriteConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.fsspec.box import (
            BoxDestinationConnector,
        )

        return BoxDestinationConnector
