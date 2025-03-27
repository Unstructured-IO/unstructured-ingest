from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.processes.connectors.weaviate.weaviate import (
    WeaviateAccessConfig,
    WeaviateConnectionConfig,
    WeaviateUploader,
    WeaviateUploaderConfig,
    WeaviateUploadStager,
    WeaviateUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from weaviate.client import WeaviateClient

CONNECTOR_TYPE = "weaviate-embedded"


class EmbeddedWeaviateAccessConfig(WeaviateAccessConfig):
    pass


class EmbeddedWeaviateConnectionConfig(WeaviateConnectionConfig):
    hostname: str = Field(default="127.0.0.1", description="hostname")
    port: int = Field(default=8079, description="http port")
    grpc_port: int = Field(default=50050, description="grpc port")
    data_path: Optional[str] = Field(
        default=None,
        description="directory where the files making up the "
        "database are stored. If not provided, will "
        "default to underlying SDK implementation",
    )
    access_config: Secret[WeaviateAccessConfig] = Field(
        default=WeaviateAccessConfig(), validate_default=True
    )

    @contextmanager
    @requires_dependencies(["weaviate"], extras="weaviate")
    def get_client(self) -> Generator["WeaviateClient", None, None]:
        from weaviate import connect_to_embedded
        from weaviate.classes.init import AdditionalConfig

        with connect_to_embedded(
            hostname=self.hostname,
            port=self.port,
            grpc_port=self.grpc_port,
            persistence_data_path=self.data_path,
            additional_config=AdditionalConfig(timeout=self.get_timeout()),
        ) as weaviate_client:
            yield weaviate_client


class EmbeddedWeaviateUploadStagerConfig(WeaviateUploadStagerConfig):
    pass


@dataclass
class EmbeddedWeaviateUploadStager(WeaviateUploadStager):
    upload_stager_config: EmbeddedWeaviateUploadStagerConfig = field(
        default_factory=lambda: WeaviateUploadStagerConfig()
    )


class EmbeddedWeaviateUploaderConfig(WeaviateUploaderConfig):
    pass


@dataclass
class EmbeddedWeaviateUploader(WeaviateUploader):
    connection_config: EmbeddedWeaviateConnectionConfig = field(
        default_factory=lambda: EmbeddedWeaviateConnectionConfig()
    )
    upload_config: EmbeddedWeaviateUploaderConfig = field(
        default_factory=lambda: EmbeddedWeaviateUploaderConfig()
    )
    connector_type: str = CONNECTOR_TYPE


weaviate_embedded_destination_entry = DestinationRegistryEntry(
    connection_config=EmbeddedWeaviateConnectionConfig,
    uploader=EmbeddedWeaviateUploader,
    uploader_config=EmbeddedWeaviateUploaderConfig,
    upload_stager=EmbeddedWeaviateUploadStager,
    upload_stager_config=EmbeddedWeaviateUploadStagerConfig,
)
