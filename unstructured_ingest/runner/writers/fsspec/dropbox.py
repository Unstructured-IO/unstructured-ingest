import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.fsspec.dropbox import DropboxWriteConfig, SimpleDropboxConfig


@dataclass
class DropboxWriter(Writer):
    connector_config: "SimpleDropboxConfig"
    write_config: "DropboxWriteConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.fsspec.dropbox import (
            DropboxDestinationConnector,
        )

        return DropboxDestinationConnector
