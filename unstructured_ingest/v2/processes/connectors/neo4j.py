import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from cymple import QueryBuilder
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
from unstructured_ingest.v2.logger import logger
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

    def run_async(
        self, elements_filepath, file_data: FileData, output_dir, output_filename, **kwargs
    ):
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
        driver = self.connection_config.get_async_driver()
        logger.info("Creating new File node")
        await self._create_file_node(driver, file_data)

        with open(path) as file:
            element_dicts = json.load(file)
            for element_dict in element_dicts:
                logger.info("Creating new Element node")
                await self._create_element_node(driver, element_dict, file_data.identifier)

        await driver.close()

    def is_async(self):
        return True

    async def _create_file_node(
        self,
        driver: "AsyncDriver",
        file_data: FileData,
    ) -> None:
        properties = {"id": file_data.identifier}

        if file_data.source_identifiers:
            properties["name"] = file_data.source_identifiers.filename
        if file_data.metadata.date_created:
            properties["date_created"] = file_data.metadata.date_created.isoformat()
        if file_data.metadata.date_modified:
            properties["date_created"] = file_data.metadata.date_modified.isoformat()

        builder = QueryBuilder()
        query = builder.create().node(labels="File", properties=properties)

        print(query)

        await driver.execute_query(query.get())

    async def _create_element_node(
        self, driver: "AsyncDriver", element_dict: dict, file_id: str
    ) -> None:
        properties = {"id": element_dict["element_id"], "text": element_dict["text"]}

        if embeddings := element_dict.get("embeddings"):
            properties["embeddings"] = embeddings

        builder = QueryBuilder()
        match_query = (
            builder.match().node(labels="File", properties={"id": file_id}, ref_name="f").with_("f")
        )
        query = match_query + (
            builder.create()
            .node(labels="Element", properties=properties)
            .related_from("element_of")
            .node(ref_name="f")
        )

        await driver.execute_query(query)


neo4j_destination_entry = DestinationRegistryEntry(
    connection_config=Neo4jConnectionConfig,
    uploader=Neo4jUploader,
    uploader_config=Neo4jUploaderConfig,
    upload_stager=Neo4jUploadStager,
    upload_stager_config=Neo4jUploadStagerConfig,
)
