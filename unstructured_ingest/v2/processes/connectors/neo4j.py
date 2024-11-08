import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator, Union

import networkx as nx
from cymple import QueryBuilder
from pydantic import BaseModel, Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.chunking import elements_from_base64_gzipped_json
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
    from neo4j import AsyncDriver, Auth


CONNECTOR_TYPE = "neo4j"


class Neo4jAccessConfig(AccessConfig):
    password: str


class Neo4jConnectionConfig(ConnectionConfig):
    access_config: Secret[Neo4jAccessConfig]
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    username: str
    uri: str = Field(description="the connection URI for the driver")

    @requires_dependencies(["neo4j"], extras="neo4j")
    def get_auth(self) -> "Auth":
        from neo4j import Auth

        return Auth("basic", self.username, self.access_config.get_secret_value().password)

    def get_drive_configs(self) -> dict:
        return {
            "uri": self.uri,
            "auth": self.get_auth(),
        }

    @requires_dependencies(["neo4j"], extras="neo4j")
    @asynccontextmanager
    async def get_client(self) -> AsyncGenerator["AsyncDriver", None]:
        from neo4j import AsyncGraphDatabase

        driver = AsyncGraphDatabase.driver(**self.get_drive_configs())
        try:
            yield driver
        finally:
            await driver.close()


class Neo4jUploadStagerConfig(UploadStagerConfig):
    pass


class Node(BaseModel):
    ref_name: str = Field(default_factory=lambda: str(uuid.uuid4()))
    labels: Union[list[str], str] = Field(default_factory=list)
    properties: dict = Field(default_factory=dict)

    def __hash__(self):
        return hash(self.ref_name)


@dataclass
class Neo4jUploadStager(UploadStager):
    upload_stager_config: Neo4jUploadStagerConfig = Field(default_factory=Neo4jUploadStagerConfig)

    def has_children(self, element: dict) -> bool:
        return "orig_elements" in element.get("metadata", {})

    def get_children(self, element: dict) -> list[dict]:
        orig_elements = element.get("metadata", {}).get("orig_elements")
        return elements_from_base64_gzipped_json(raw_s=orig_elements)

    def element_to_node(self, element: dict) -> Node:
        properties = {"id": element["element_id"], "text": element["text"]}

        if embeddings := element.get("embeddings"):
            properties["embeddings"] = embeddings

        return Node(
            properties=properties,
        )

    def create_graph(self, elements: list[dict], parent_node: Node) -> nx.Graph:
        graph = nx.Graph()
        graph.add_node(parent_node)
        for element in elements:
            element_type = element["type"]
            element_type_node = Node(ref_name=element_type)
            if element_type_node not in graph.nodes:
                graph.add_node(element_type_node)
            element_node = self.element_to_node(element=element)
            graph.add_edge(element_node, element_type_node)
            if self.has_children(element):
                element_node.labels.append("chunked_element")
                graph.add_node(element_node)
                for child in self.get_children(element):
                    child_element_node = self.element_to_node(element=child)
                    graph.add_node(child_element_node)
                    graph.add_edge(element_node, child_element_node)
            graph.add_edge(element_node, parent_node)
        return graph

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with elements_filepath.open() as file:
            elements = json.load(file)
        file_node = Node(ref_name="file")
        graph = self.create_graph(elements, file_node)
        nodes = graph.nodes()
        file_node.model_dump()
        nodes_dict = [n.model_dump() for n in nodes]
        edges = list(graph.edges())
        edges_dict = [[edge[0].ref_name, edge[1].ref_name] for edge in edges]
        data = {"nodes": nodes_dict, "edges": edges_dict}
        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as output_file:
            json.dump(data, output_file, indent=2)
        return output_path


class Neo4jUploaderConfig(UploaderConfig):
    database: str = Field(description="database to write to")


@dataclass
class Neo4jUploader(Uploader):
    upload_config: Neo4jUploaderConfig
    connection_config: Neo4jConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self):
        return True

    @DestinationConnectionError.wrap
    def precheck(self) -> None:
        async def verify_auth():
            async with self.connection_config.get_client() as client:
                if not client.verify_authentication():
                    raise DestinationConnectionError("Invalid authentication information.")

        asyncio.run(verify_auth())

    def get_elements(self, path: Path) -> dict:
        with path.open() as file:
            return json.load(file)

    async def run_async(self, path: Path, file_data: FileData, **kwargs) -> None:
        async with self.connection_config.get_client() as client:
            logger.info("Creating new file node")
            await self._create_file_node(client, file_data)

            elements = self.get_elements(path=path)
            file_id = file_data.identifier
            await asyncio.gather(
                *[
                    self._create_element_node(
                        client=client, element_dict=element_dict, file_id=file_id
                    )
                    for element_dict in elements
                ]
            )

    async def _create_file_node(
        self,
        client: "AsyncDriver",
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

        await client.execute_query(query.get())

    def get_element_properties(self, element_dict) -> dict:
        properties = {"id": element_dict["element_id"], "text": element_dict["text"]}

        if embeddings := element_dict.get("embeddings"):
            properties["embeddings"] = embeddings

        return properties

    async def _create_element_node(
        self, client: "AsyncDriver", element_dict: dict, file_id: str
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

        await client.execute_query(query)


neo4j_destination_entry = DestinationRegistryEntry(
    connection_config=Neo4jConnectionConfig,
    uploader=Neo4jUploader,
    uploader_config=Neo4jUploaderConfig,
)
