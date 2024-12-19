from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generator

from pydantic import Field, Secret

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.v2.processes.connectors.weaviate.weaviate import (
    WeaviateAccessConfig,
    WeaviateConnectionConfig,
    WeaviateUploader,
    WeaviateUploaderConfig,
    WeaviateUploadStager,
    WeaviateUploadStagerConfig,
)

if TYPE_CHECKING:
    from weaviate.client import WeaviateClient

CONNECTOR_TYPE = "weaviate-local"


class LocalWeaviateAccessConfig(WeaviateAccessConfig):
    pass


class LocalWeaviateConnectionConfig(WeaviateConnectionConfig):
    access_config: Secret[WeaviateAccessConfig] = Field(
        default=WeaviateAccessConfig(), validate_default=True
    )

    @contextmanager
    @requires_dependencies(["weaviate"], extras="weaviate")
    def get_client(self) -> Generator["WeaviateClient", None, None]:
        from weaviate import connect_to_local
        from weaviate.classes.init import AdditionalConfig

        with connect_to_local(
            additional_config=AdditionalConfig(timeout=self.get_timeout())
        ) as weaviate_client:
            yield weaviate_client


class LocalWeaviateUploadStagerConfig(WeaviateUploadStagerConfig):
    pass


@dataclass
class LocalWeaviateUploadStager(WeaviateUploadStager):
    upload_stager_config: LocalWeaviateUploadStagerConfig = field(
        default_factory=lambda: WeaviateUploadStagerConfig()
    )


class LocalWeaviateUploaderConfig(WeaviateUploaderConfig):
    pass


@dataclass
class LocalWeaviateUploader(WeaviateUploader):
    upload_config: LocalWeaviateUploaderConfig
    connector_type: str = CONNECTOR_TYPE
    connection_config: LocalWeaviateConnectionConfig


weaviate_local_destination_entry = DestinationRegistryEntry(
    connection_config=LocalWeaviateConnectionConfig,
    uploader=LocalWeaviateUploader,
    uploader_config=LocalWeaviateUploaderConfig,
    upload_stager=LocalWeaviateUploadStager,
    upload_stager_config=LocalWeaviateUploadStagerConfig,
)
