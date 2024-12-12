from __future__ import annotations

import asyncio
import json
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional

import networkx as nx
from dateutil import parser
from pydantic import BaseModel, ConfigDict, Field, Secret

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
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
)

if TYPE_CHECKING:
    from neo4j import AsyncDriver, Auth

CONNECTOR_TYPE = "neo4j"


class Label(Enum):
    UNSTRUCTURED_ELEMENT = "UnstructuredElement"
    CHUNK = "Chunk"
    DOCUMENT = "Document"


class Relationship(Enum):
    PART_OF_DOCUMENT = "PART_OF_DOCUMENT"
    PART_OF_CHUNK = "PART_OF_CHUNK"
    NEXT_CHUNK = "NEXT_CHUNK"
    NEXT_ELEMENT = "NEXT_ELEMENT"


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
    model_config = ConfigDict(use_enum_values=True)

    id_: str = Field(default_factory=lambda: str(uuid.uuid4()))
    labels: list[Label] = Field(default_factory=list)
    properties: dict = Field(default_factory=dict)

    def __hash__(self):
        return hash(self.id_)


class Edge(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    source_id: str
    destination_id: str
    relationship: Relationship


class GraphData(BaseModel):
    nodes: list[Node]
    edges: list[Edge]

    @classmethod
    def from_nx(cls, nx_graph: nx.MultiDiGraph) -> GraphData:
        nodes = list(nx_graph.nodes())
        edges = [
            Edge(
                source_id=u.id_,
                destination_id=v.id_,
                relationship=Relationship(data_dict["relationship"]),
            )
            for u, v, data_dict in nx_graph.edges(data=True)
        ]
        return GraphData(nodes=nodes, edges=edges)


@dataclass
class Neo4jUploadStager(UploadStager):
    upload_stager_config: Neo4jUploadStagerConfig = Field(
        default_factory=Neo4jUploadStagerConfig, validate_default=True
    )

    def run(  # type: ignore
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with elements_filepath.open() as file:
            elements = json.load(file)

        nx_graph = self._create_lexical_graph(
            elements, self._create_document_node(file_data=file_data)
        )
        output_filepath = Path(output_dir) / f"{output_filename}.json"
        output_filepath.parent.mkdir(parents=True, exist_ok=True)

        with open(output_filepath, "w") as file:
            json.dump(GraphData.from_nx(nx_graph).model_dump(), file, indent=4)

        return output_filepath

    def _create_lexical_graph(self, elements: list[dict], document_node: Node) -> nx.Graph:
        graph = nx.MultiDiGraph()
        graph.add_node(document_node)

        previous_node: Optional[Node] = None
        # TODO(Filip Knefel): Consolidate the nodes create/add code in this loop
        for element in elements:
            if self._is_chunk(element):
                chunk_node = self._create_element_node(element)
                if previous_node:
                    graph.add_edge(
                        chunk_node, previous_node, relationship=Relationship.NEXT_CHUNK.value
                    )
                previous_node = chunk_node
                graph.add_edge(
                    chunk_node, document_node, relationship=Relationship.PART_OF_DOCUMENT.value
                )
                origin_element_nodes = [
                    self._create_element_node(origin_element)
                    for origin_element in self._get_origin_elements(element)
                ]
                graph.add_edges_from(
                    [
                        (origin_element_node, chunk_node)
                        for origin_element_node in origin_element_nodes
                    ],
                    relationship=Relationship.PART_OF_CHUNK.value,
                )
            else:
                element_node = self._create_element_node(element)
                graph.add_edge(
                    element_node, document_node, relationship=Relationship.PART_OF_DOCUMENT.value
                )
                if previous_node:
                    graph.add_edge(
                        element_node, previous_node, relationship=Relationship.NEXT_CHUNK.value
                    )
                previous_node = element_node

        return graph

    # TODO(Filip Knefel): Ensure _is_chunk is as reliable as possible, consider different checks
    def _is_chunk(self, element: dict) -> bool:
        return "orig_elements" in element.get("metadata", {})

    def _create_document_node(self, file_data: FileData) -> Node:
        properties = {
            "name": file_data.source_identifiers.filename,
        }
        if date_created := file_data.metadata.date_created:
            properties["date_created"] = parser.parse(date_created).isoformat()
        if date_modified := file_data.metadata.date_modified:
            properties["date_modified"] = parser.parse(date_modified).isoformat()
        return Node(id_=file_data.identifier, properties=properties, labels=[Label.DOCUMENT])

    def _create_element_node(self, element: dict) -> Node:
        properties = {"id": element["element_id"], "text": element["text"]}

        if embeddings := element.get("embeddings"):
            properties["embeddings"] = embeddings

        label = Label.CHUNK if self._is_chunk(element) else Label.UNSTRUCTURED_ELEMENT
        return Node(id_=element["element_id"], properties=properties, labels=[label])

    def _get_origin_elements(self, chunk_element: dict) -> list[dict]:
        orig_elements = chunk_element.get("metadata", {}).get("orig_elements")
        return elements_from_base64_gzipped_json(raw_s=orig_elements)


class Neo4jUploaderConfig(UploaderConfig):
    database: str = Field(description="database to write to")
    batch_size: int = Field(default=100, description="batch size")


@dataclass
class Neo4jUploader(Uploader):
    upload_config: Neo4jUploaderConfig
    connection_config: Neo4jConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @DestinationConnectionError.wrap
    def precheck(self) -> None:
        async def verify_auth():
            async with self.connection_config.get_client() as client:
                if not client.verify_authentication():
                    raise DestinationConnectionError("Invalid authentication information.")

        asyncio.run(verify_auth())

    def is_async(self):
        return True

    async def run_async(self, path: Path, file_data: FileData, **kwargs) -> None:  # type: ignore
        with path.open() as file:
            staged_data = json.load(file)

        graph_data = GraphData.model_validate(staged_data)

        async with self.connection_config.get_client() as client:
            await self._merge_graph(graph_data=graph_data, client=client)

    async def _merge_graph(self, graph_data: GraphData, client: AsyncDriver) -> None:
        nodes_by_labels: defaultdict[tuple[Label, ...], list[Node]] = defaultdict(list)
        for node in graph_data.nodes:
            nodes_by_labels[tuple(node.labels)].append(node)

        await self._execute_queries(
            [self._create_nodes_query(nodes, labels) for labels, nodes in nodes_by_labels.items()],
            client=client,
            in_parallel=True,
        )

        edges_by_relationship: defaultdict[Relationship, list[Edge]] = defaultdict(list)
        for edge in graph_data.edges:
            edges_by_relationship[edge.relationship].append(edge)

        await self._execute_queries(
            [
                self._create_edges_query(edges, relationship)
                for relationship, edges in edges_by_relationship.items()
            ],
            client=client,
        )

    @staticmethod
    async def _execute_queries(
        queries_with_parameters: list[tuple[str, dict]],
        client: AsyncDriver,
        in_parallel: bool = True,
    ) -> None:
        if in_parallel:
            asyncio.gather(
                *[
                    client.execute_query(query, parameters_=parameters)
                    for query, parameters in queries_with_parameters
                ]
            )
        else:
            for query, parameters in queries_with_parameters:
                await client.execute_query(query, parameters_=parameters)

    @staticmethod
    def _create_nodes_query(nodes: list[Node], labels: tuple[Label, ...]) -> tuple[str, dict]:
        labels_string = ", ".join(labels)
        query_string = f"""
            UNWIND $nodes AS node
            MERGE (n: {labels_string} {{id: node.id}})
            SET n += node.properties
            """
        parameters = {"nodes": [{"id": node.id_, "properties": node.properties} for node in nodes]}
        return query_string, parameters

    @staticmethod
    def _create_edges_query(edges: list[Edge], relationship: Relationship) -> tuple[str, dict]:
        query_string = f"""
            UNWIND $edges AS edge
            MATCH (u {{id: edge.source}}), (v {{id: edge.destination}})
            MERGE (u)-[:{relationship}]->(v)
            """
        parameters = {
            "edges": [
                {"source": edge.source_id, "destination": edge.destination_id} for edge in edges
            ]
        }
        return query_string, parameters


neo4j_destination_entry = DestinationRegistryEntry(
    connection_config=Neo4jConnectionConfig,
    uploader=Neo4jUploader,
    uploader_config=Neo4jUploaderConfig,
)
