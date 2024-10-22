from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
)

if TYPE_CHECKING:
    from neo4j import AsyncDriver, Driver


CONNECTOR_TYPE = "neo4j"


class Neo4jAccessConfig(AccessConfig):
    uri: str
    user: str
    password: str


class Neo4jConnectionConfig(ConnectionConfig):
    access_config: Secret[Neo4jAccessConfig]
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    database: Optional[str] = Field(default=None)

    @requires_dependencies(["neo4j"], extras="neo4j")
    @DestinationConnectionError.wrap
    def get_driver(self) -> "Driver":
        from neo4j import GraphDatabase

        access_config = self.access_config.get_secret_value()
        return GraphDatabase.driver(
            uri=access_config.uri,
            auth=(access_config.user, access_config.password),
            database=self.database,
        )

    @requires_dependencies(["neo4j"], extras="neo4j")
    @DestinationConnectionError.wrap
    @lru_cache(maxsize=1)
    def get_async_driver(self) -> "AsyncDriver":
        from neo4j import AsyncGraphDatabase

        access_config = self.access_config.get_secret_value()
        return AsyncGraphDatabase.driver(
            uri=access_config.uri,
            auth=(access_config.user, access_config.password),
            database=self.database,
        )


class Neo4jUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class Neo4jUploadStager(UploadStager):
    upload_stager_config: Neo4jUploadStagerConfig = field(
        default_factory=lambda: Neo4jUploadStagerConfig()
    )

    def is_async(self):
        return True

    def run_async(self, elements_filepath, file_data, output_dir, output_filename, **kwargs):
        return super().run_async(
            elements_filepath, file_data, output_dir, output_filename, **kwargs
        )


class Neo4jUploaderConfig(UploaderConfig):
    pass


@dataclass
class Neo4jUploader(Uploader):
    upload_config: Neo4jUploaderConfig
    connection_config: Neo4jConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @DestinationConnectionError.wrap
    def precheck(self) -> None:
        driver = self.connection_config.get_driver()
        if not driver.verify_authentication():
            raise DestinationConnectionError("Invalid authentication information.")

    def run(self, path, file_data, **kwargs):
        raise NotImplementedError

    async def run_async(self, path: Path, file_data: FileData, **kwargs) -> None:  # type: ignore[override]
        raise NotImplementedError


neo4j_destination_entry = DestinationRegistryEntry(
    connection_config=Neo4jConnectionConfig,
    uploader=Neo4jUploader,
    uploader_config=Neo4jUploaderConfig,
    upload_stager=Neo4jUploadStager,
    upload_stager_config=Neo4jUploadStagerConfig,
)
