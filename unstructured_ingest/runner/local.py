import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseSourceConnector
from unstructured_ingest.runner.base_runner import Runner

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.local import SimpleLocalConfig


@dataclass
class LocalRunner(Runner):
    connector_config: "SimpleLocalConfig"

    def update_read_config(self):
        pass

    def get_source_connector_cls(self) -> t.Type[BaseSourceConnector]:
        from unstructured_ingest.connector.local import (
            LocalSourceConnector,
        )

        return LocalSourceConnector
