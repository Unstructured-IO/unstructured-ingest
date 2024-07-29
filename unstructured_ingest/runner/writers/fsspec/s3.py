import typing as t
from dataclasses import dataclass

from unstructured_ingest.interfaces import BaseDestinationConnector
from unstructured_ingest.runner.writers.base_writer import Writer

if t.TYPE_CHECKING:
    from unstructured_ingest.connector.fsspec.s3 import S3WriteConfig, SimpleS3Config


@dataclass
class S3Writer(Writer):
    connector_config: "SimpleS3Config"
    write_config: "S3WriteConfig"

    def get_connector_cls(self) -> t.Type[BaseDestinationConnector]:
        from unstructured_ingest.connector.fsspec.s3 import (
            S3DestinationConnector,
        )

        return S3DestinationConnector
