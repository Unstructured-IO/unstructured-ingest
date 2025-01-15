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

from pydantic import BaseModel, ConfigDict, Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.chunking import elements_from_base64_gzipped_json
from unstructured_ingest.utils.data_prep import batch_generator
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
    from networkx import Graph, MultiDiGraph

CONNECTOR_TYPE = "neo4j"


class Neo4jAccessConfig(AccessConfig):
    password: str


class Neo4jConnectionConfig(ConnectionConfig):
    access_config: Secret[Neo4jAccessConfig]
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    username: str
    uri: str = Field(description="Neo4j Connection URI <scheme>://<host>:<port>")
    database: str = Field(description="Name of the target database")

    @requires_dependencies(["neo4j"], extras="neo4j")
    @asynccontextmanager
    async def get_client(self) -> AsyncGenerator["AsyncDriver", None]:
        from neo4j import AsyncGraphDatabase

        driver = AsyncGraphDatabase.driver(**self._get_driver_parameters())
        logger.info(f"Created driver connecting to the database '{self.database}' at {self.uri}.")
        try:
            yield driver
        finally:
            await driver.close()
            logger.info(
                f"Closed driver connecting to the database '{self.database}' at {self.uri}."
            )

    def _get_driver_parameters(self) -> dict:
        return {
            "uri": self.uri,
            "auth": self._get_auth(),
            "database": self.database,
        }

    @requires_dependencies(["neo4j"], extras="neo4j")
    def _get_auth(self) -> "Auth":
        from neo4j import Auth

        return Auth("basic", self.username, self.access_config.get_secret_value().password)


class Neo4jUploadStagerConfig(UploadStagerConfig):
    pass


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
            file.write(_GraphData.from_nx(nx_graph).model_dump_json())

        return output_filepath

    def _create_lexical_graph(self, elements: list[dict], document_node: _Node) -> "Graph":
        import networkx as nx

        graph = nx.MultiDiGraph()
        graph.add_node(document_node)

        previous_node: Optional[_Node] = None
        for element in elements:
            element_node = self._create_element_node(element)
            order_relationship = (
                Relationship.NEXT_CHUNK if self._is_chunk(element) else Relationship.NEXT_ELEMENT
            )
            if previous_node:
                graph.add_edge(element_node, previous_node, relationship=order_relationship)

            previous_node = element_node
            graph.add_edge(element_node, document_node, relationship=Relationship.PART_OF_DOCUMENT)

            if self._is_chunk(element):
                origin_element_nodes = [
                    self._create_element_node(origin_element)
                    for origin_element in self._get_origin_elements(element)
                ]
                graph.add_edges_from(
                    [
                        (origin_element_node, element_node)
                        for origin_element_node in origin_element_nodes
                    ],
                    relationship=Relationship.PART_OF_CHUNK,
                )
                graph.add_edges_from(
                    [
                        (origin_element_node, document_node)
                        for origin_element_node in origin_element_nodes
                    ],
                    relationship=Relationship.PART_OF_DOCUMENT,
                )

        return graph

    # TODO(Filip Knefel): Ensure _is_chunk is as reliable as possible, consider different checks
    def _is_chunk(self, element: dict) -> bool:
        return "orig_elements" in element.get("metadata", {})

    def _create_document_node(self, file_data: FileData) -> _Node:
        properties = {}
        if file_data.source_identifiers:
            properties["name"] = file_data.source_identifiers.filename
        if file_data.metadata.date_created:
            properties["date_created"] = file_data.metadata.date_created
        if file_data.metadata.date_modified:
            properties["date_modified"] = file_data.metadata.date_modified
        return _Node(id_=file_data.identifier, properties=properties, labels=[Label.DOCUMENT])

    def _create_element_node(self, element: dict) -> _Node:
        properties = {"id": element["element_id"], "text": element["text"]}

        if embeddings := element.get("embeddings"):
            properties["embeddings"] = embeddings

        label = Label.CHUNK if self._is_chunk(element) else Label.UNSTRUCTURED_ELEMENT
        return _Node(id_=element["element_id"], properties=properties, labels=[label])

    def _get_origin_elements(self, chunk_element: dict) -> list[dict]:
        orig_elements = chunk_element.get("metadata", {}).get("orig_elements")
        return elements_from_base64_gzipped_json(raw_s=orig_elements)


class _GraphData(BaseModel):
    nodes: list[_Node]
    edges: list[_Edge]

    @classmethod
    def from_nx(cls, nx_graph: "MultiDiGraph") -> _GraphData:
        nodes = list(nx_graph.nodes())
        edges = [
            _Edge(
                source_id=u.id_,
                destination_id=v.id_,
                relationship=Relationship(data_dict["relationship"]),
            )
            for u, v, data_dict in nx_graph.edges(data=True)
        ]
        return _GraphData(nodes=nodes, edges=edges)


class _Node(BaseModel):
    model_config = ConfigDict()

    id_: str = Field(default_factory=lambda: str(uuid.uuid4()))
    labels: list[Label] = Field(default_factory=list)
    properties: dict = Field(default_factory=dict)

    def __hash__(self):
        return hash(self.id_)


class _Edge(BaseModel):
    model_config = ConfigDict()

    source_id: str
    destination_id: str
    relationship: Relationship


class Label(Enum):
    UNSTRUCTURED_ELEMENT = "UnstructuredElement"
    CHUNK = "Chunk"
    DOCUMENT = "Document"


class Relationship(Enum):
    PART_OF_DOCUMENT = "PART_OF_DOCUMENT"
    PART_OF_CHUNK = "PART_OF_CHUNK"
    NEXT_CHUNK = "NEXT_CHUNK"
    NEXT_ELEMENT = "NEXT_ELEMENT"


class Neo4jUploaderConfig(UploaderConfig):
    batch_size: int = Field(
        default=100, description="Maximal number of nodes/relationships created per transaction."
    )


@dataclass
class Neo4jUploader(Uploader):
    upload_config: Neo4jUploaderConfig
    connection_config: Neo4jConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @DestinationConnectionError.wrap
    def precheck(self) -> None:
        async def verify_auth():
            async with self.connection_config.get_client() as client:
                await client.verify_connectivity()

        asyncio.run(verify_auth())

    def is_async(self):
        return True

    async def run_async(self, path: Path, file_data: FileData, **kwargs) -> None:  # type: ignore
        with path.open() as file:
            staged_data = json.load(file)

        graph_data = _GraphData.model_validate(staged_data)
        async with self.connection_config.get_client() as client:
            await self._create_uniqueness_constraints(client)
            await self._delete_old_data_if_exists(file_data, client=client)
            await self._merge_graph(graph_data=graph_data, client=client)

    async def _create_uniqueness_constraints(self, client: AsyncDriver) -> None:
        for label in Label:
            logger.info(
                f"Adding id uniqueness constraint for nodes labeled '{label.value}'"
                " if it does not already exist."
            )
            constraint_name = f"{label.value.lower()}_id"
            await client.execute_query(
                f"""
                CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
                FOR (n: {label.value}) REQUIRE n.id IS UNIQUE
                """
            )

    async def _delete_old_data_if_exists(self, file_data: FileData, client: AsyncDriver) -> None:
        logger.info(f"Deleting old data for the record '{file_data.identifier}' (if present).")
        _, summary, _ = await client.execute_query(
            f"""
            MATCH (n: {Label.DOCUMENT.value} {{id: $identifier}})
            MATCH (n)--(m: {Label.CHUNK.value}|{Label.UNSTRUCTURED_ELEMENT.value})
            DETACH DELETE m""",
            identifier=file_data.identifier,
        )
        logger.info(
            f"Deleted {summary.counters.nodes_deleted} nodes"
            f" and {summary.counters.relationships_deleted} relationships."
        )

    async def _merge_graph(self, graph_data: _GraphData, client: AsyncDriver) -> None:
        nodes_by_labels: defaultdict[tuple[Label, ...], list[_Node]] = defaultdict(list)
        for node in graph_data.nodes:
            nodes_by_labels[tuple(node.labels)].append(node)

        logger.info(f"Merging {len(graph_data.nodes)} graph nodes.")
        # NOTE: Processed in parallel as there's no overlap between accessed nodes
        await self._execute_queries(
            [
                self._create_nodes_query(nodes_batch, labels)
                for labels, nodes in nodes_by_labels.items()
                for nodes_batch in batch_generator(nodes, batch_size=self.upload_config.batch_size)
            ],
            client=client,
            in_parallel=True,
        )
        logger.info(f"Finished merging {len(graph_data.nodes)} graph nodes.")

        edges_by_relationship: defaultdict[Relationship, list[_Edge]] = defaultdict(list)
        for edge in graph_data.edges:
            edges_by_relationship[edge.relationship].append(edge)

        logger.info(f"Merging {len(graph_data.edges)} graph relationships (edges).")
        # NOTE: Processed sequentially to avoid queries locking node access to one another
        await self._execute_queries(
            [
                self._create_edges_query(edges_batch, relationship)
                for relationship, edges in edges_by_relationship.items()
                for edges_batch in batch_generator(edges, batch_size=self.upload_config.batch_size)
            ],
            client=client,
        )
        logger.info(f"Finished merging {len(graph_data.edges)} graph relationships (edges).")

    @staticmethod
    async def _execute_queries(
        queries_with_parameters: list[tuple[str, dict]],
        client: AsyncDriver,
        in_parallel: bool = False,
    ) -> None:
        if in_parallel:
            logger.info(f"Executing {len(queries_with_parameters)} queries in parallel.")
            await asyncio.gather(
                *[
                    client.execute_query(query, parameters_=parameters)
                    for query, parameters in queries_with_parameters
                ]
            )
            logger.info("Finished executing parallel queries.")
        else:
            logger.info(f"Executing {len(queries_with_parameters)} queries sequentially.")
            for i, (query, parameters) in enumerate(queries_with_parameters):
                logger.info(f"Query #{i} started.")
                await client.execute_query(query, parameters_=parameters)
                logger.info(f"Query #{i} finished.")
            logger.info(
                f"Finished executing all ({len(queries_with_parameters)}) sequential queries."
            )

    @staticmethod
    def _create_nodes_query(nodes: list[_Node], labels: tuple[Label, ...]) -> tuple[str, dict]:
        labels_string = ", ".join([label.value for label in labels])
        logger.info(f"Preparing MERGE query for {len(nodes)} nodes labeled '{labels_string}'.")
        query_string = f"""
            UNWIND $nodes AS node
            MERGE (n: {labels_string} {{id: node.id}})
            SET n += node.properties
            """
        parameters = {"nodes": [{"id": node.id_, "properties": node.properties} for node in nodes]}
        return query_string, parameters

    @staticmethod
    def _create_edges_query(edges: list[_Edge], relationship: Relationship) -> tuple[str, dict]:
        logger.info(f"Preparing MERGE query for {len(edges)} {relationship} relationships.")
        query_string = f"""
            UNWIND $edges AS edge
            MATCH (u {{id: edge.source}})
            MATCH (v {{id: edge.destination}})
            MERGE (u)-[:{relationship.value}]->(v)
            """
        parameters = {
            "edges": [
                {"source": edge.source_id, "destination": edge.destination_id} for edge in edges
            ]
        }
        return query_string, parameters


neo4j_destination_entry = DestinationRegistryEntry(
    connection_config=Neo4jConnectionConfig,
    upload_stager=Neo4jUploadStager,
    upload_stager_config=Neo4jUploadStagerConfig,
    uploader=Neo4jUploader,
    uploader_config=Neo4jUploaderConfig,
)
