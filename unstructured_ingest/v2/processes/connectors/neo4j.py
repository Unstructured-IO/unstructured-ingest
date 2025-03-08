from __future__ import annotations

import asyncio
import json
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, Secret, field_validator

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.logger import logger
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
from unstructured_ingest.v2.processes.connectors.utils import format_and_truncate_orig_elements

SimilarityFunction = Literal["cosine"]

if TYPE_CHECKING:
    from neo4j import AsyncDriver, Auth
    from networkx import Graph, MultiDiGraph

CONNECTOR_TYPE = "neo4j"


class Neo4jAccessConfig(AccessConfig):
    password: str


class Neo4jConnectionConfig(ConnectionConfig):
    access_config: Secret[Neo4jAccessConfig]
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    username: str = Field(default="neo4j")
    uri: str = Field(description="Neo4j Connection URI <scheme>://<host>:<port>")
    database: str = Field(default="neo4j", description="Name of the target database")

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

    def _add_entities(self, element: dict, graph: "Graph", element_node: _Node) -> None:
        entities = element.get("metadata", {}).get("entities", [])
        if not entities:
            return None
        if not isinstance(entities, list):
            return None

        for entity in entities:
            if not isinstance(entity, dict):
                continue
            if "entity" not in entity or "type" not in entity:
                continue
            entity_node = _Node(
                labels=[Label.ENTITY], properties={"id": entity["entity"]}, id_=entity["entity"]
            )
            graph.add_edge(
                entity_node,
                _Node(labels=[Label.ENTITY], properties={"id": entity["type"]}, id_=entity["type"]),
                relationship=Relationship.ENTITY_TYPE,
            )
            graph.add_edge(element_node, entity_node, relationship=Relationship.HAS_ENTITY)

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

            self._add_entities(element, graph, element_node)

            if self._is_chunk(element):
                for origin_element in format_and_truncate_orig_elements(element, include_text=True):
                    origin_element_node = self._create_element_node(origin_element)

                    graph.add_edge(
                        origin_element_node,
                        element_node,
                        relationship=Relationship.PART_OF_CHUNK,
                    )
                    graph.add_edge(
                        origin_element_node,
                        document_node,
                        relationship=Relationship.PART_OF_DOCUMENT,
                    )
                    self._add_entities(origin_element, graph, origin_element_node)

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
        properties = {"id": element["element_id"]}

        if text := element.get("text"):
            # if we have chunks, we won't have text here for the original elements
            properties["text"] = text

        if embeddings := element.get("embeddings"):
            properties["embeddings"] = embeddings

        label = Label.CHUNK if self._is_chunk(element) else Label.UNSTRUCTURED_ELEMENT
        return _Node(id_=element["element_id"], properties=properties, labels=[label])


class _GraphData(BaseModel):
    nodes: list[_Node]
    edges: list[_Edge]

    @classmethod
    def from_nx(cls, nx_graph: "MultiDiGraph") -> _GraphData:
        nodes = list(nx_graph.nodes())
        edges = [
            _Edge(
                source=u,
                destination=v,
                relationship=Relationship(data_dict["relationship"]),
            )
            for u, v, data_dict in nx_graph.edges(data=True)
        ]
        return _GraphData(nodes=nodes, edges=edges)


class _Node(BaseModel):
    model_config = ConfigDict()

    labels: list[Label]
    properties: dict = Field(default_factory=dict)
    id_: str = Field(default_factory=lambda: str(uuid.uuid4()))

    def __hash__(self):
        return hash(self.id_)

    @property
    def main_label(self) -> Label:
        return self.labels[0]

    @classmethod
    @field_validator("labels", mode="after")
    def require_at_least_one_label(cls, value: list[Label]) -> list[Label]:
        if not value:
            raise ValueError("Node must have at least one label.")
        return value


class _Edge(BaseModel):
    model_config = ConfigDict()

    source: _Node
    destination: _Node
    relationship: Relationship


class Label(Enum):
    UNSTRUCTURED_ELEMENT = "UnstructuredElement"
    CHUNK = "Chunk"
    DOCUMENT = "Document"
    ENTITY = "Entity"


class Relationship(Enum):
    PART_OF_DOCUMENT = "PART_OF_DOCUMENT"
    PART_OF_CHUNK = "PART_OF_CHUNK"
    NEXT_CHUNK = "NEXT_CHUNK"
    NEXT_ELEMENT = "NEXT_ELEMENT"
    ENTITY_TYPE = "ENTITY_TYPE"
    HAS_ENTITY = "HAS_ENTITY"


class Neo4jUploaderConfig(UploaderConfig):
    batch_size: int = Field(
        default=1000, description="Maximal number of nodes/relationships created per transaction."
    )
    similarity_function: SimilarityFunction = Field(
        default="cosine",
        description="Vector similarity function used to create index on Chunk nodes",
    )
    create_destination: bool = Field(
        default=True, description="Create destination if it does not exist"
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
            embedding_dimensions = self._get_embedding_dimensions(graph_data)
            if embedding_dimensions and self.upload_config.create_destination:
                await self._create_vector_index(
                    client,
                    dimensions=embedding_dimensions,
                    similarity_function=self.upload_config.similarity_function,
                )
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

    async def _create_vector_index(
        self, client: AsyncDriver, dimensions: int, similarity_function: SimilarityFunction
    ) -> None:
        import neo4j.exceptions

        label = Label.CHUNK
        logger.info(
            f"Creating index on nodes labeled '{label.value}' if it does not already exist."
        )
        index_name = f"{label.value.lower()}_vector"
        try:
            await client.execute_query(
                f"""
            CREATE VECTOR INDEX {index_name} IF NOT EXISTS
            FOR (n:{label.value}) ON n.embedding
            OPTIONS {{indexConfig: {{
                `vector.similarity_function`: '{similarity_function}',
                `vector.dimensions`: {dimensions}}}
            }}
            """
            )
        except neo4j.exceptions.ClientError as e:
            if e.code == "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists":
                logger.info(f"Index on nodes labeled '{label.value}' already exists.")
            else:
                raise

    async def _delete_old_data_if_exists(self, file_data: FileData, client: AsyncDriver) -> None:
        logger.info(f"Deleting old data for the record '{file_data.identifier}' (if present).")
        _, summary, _ = await client.execute_query(
            f"""
            MATCH (n: `{Label.DOCUMENT.value}` {{id: $identifier}})
            MATCH (n)--(m: `{Label.CHUNK.value}`|`{Label.UNSTRUCTURED_ELEMENT.value}`)
            DETACH DELETE m
            DETACH DELETE n""",
            identifier=file_data.identifier,
        )
        logger.info(
            f"Deleted {summary.counters.nodes_deleted} nodes"
            f" and {summary.counters.relationships_deleted} relationships."
        )

    async def _merge_graph(self, graph_data: _GraphData, client: AsyncDriver) -> None:
        nodes_by_labels: defaultdict[Label, list[_Node]] = defaultdict(list)
        for node in graph_data.nodes:
            nodes_by_labels[node.main_label].append(node)
        logger.info(f"Merging {len(graph_data.nodes)} graph nodes.")
        # NOTE: Processed in parallel as there's no overlap between accessed nodes
        await self._execute_queries(
            [
                self._create_nodes_query(nodes_batch, label)
                for label, nodes in nodes_by_labels.items()
                for nodes_batch in batch_generator(nodes, batch_size=self.upload_config.batch_size)
            ],
            client=client,
            in_parallel=True,
        )
        logger.info(f"Finished merging {len(graph_data.nodes)} graph nodes.")

        edges_by_relationship: defaultdict[tuple[Relationship, Label, Label], list[_Edge]] = (
            defaultdict(list)
        )
        for edge in graph_data.edges:
            key = (edge.relationship, edge.source.main_label, edge.destination.main_label)
            edges_by_relationship[key].append(edge)

        logger.info(f"Merging {len(graph_data.edges)} graph relationships (edges).")
        # NOTE: Processed sequentially to avoid queries locking node access to one another
        await self._execute_queries(
            [
                self._create_edges_query(edges_batch, relationship, source_label, destination_label)
                for (
                    relationship,
                    source_label,
                    destination_label,
                ), edges in edges_by_relationship.items()
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
        from neo4j import EagerResult

        results: list[EagerResult] = []
        logger.info(
            f"Executing {len(queries_with_parameters)} "
            + f"{'parallel' if in_parallel else 'sequential'} Cypher statements."
        )
        if in_parallel:
            results = await asyncio.gather(
                *[
                    client.execute_query(query, parameters_=parameters)
                    for query, parameters in queries_with_parameters
                ]
            )
        else:
            for i, (query, parameters) in enumerate(queries_with_parameters):
                logger.info(f"Statement #{i} started.")
                results.append(await client.execute_query(query, parameters_=parameters))
                logger.info(f"Statement #{i} finished.")
        nodeCount = sum([res.summary.counters.nodes_created for res in results])
        relCount = sum([res.summary.counters.relationships_created for res in results])
        logger.info(
            f"Finished executing all ({len(queries_with_parameters)}) "
            + f"{'parallel' if in_parallel else 'sequential'} Cypher statements. "
            + f"Created {nodeCount} nodes, {relCount} relationships."
        )

    @staticmethod
    def _create_nodes_query(nodes: list[_Node], label: Label) -> tuple[str, dict]:
        logger.info(f"Preparing MERGE query for {len(nodes)} nodes labeled '{label}'.")
        query_string = f"""
            UNWIND $nodes AS node
            MERGE (n: `{label.value}` {{id: node.id}})
            SET n += node.properties
            SET n:$(node.labels)
            WITH * WHERE node.vector IS NOT NULL
            CALL db.create.setNodeVectorProperty(n, 'embedding', node.vector)
            """
        parameters = {
            "nodes": [
                {
                    "id": node.id_,
                    "labels": [l.value for l in node.labels if l != label],  # noqa: E741
                    "vector": node.properties.pop("embedding", None),
                    "properties": node.properties,
                }
                for node in nodes
            ]
        }
        return query_string, parameters

    @staticmethod
    def _create_edges_query(
        edges: list[_Edge],
        relationship: Relationship,
        source_label: Label,
        destination_label: Label,
    ) -> tuple[str, dict]:
        logger.info(f"Preparing MERGE query for {len(edges)} {relationship} relationships.")
        query_string = f"""
            UNWIND $edges AS edge
            MATCH (u: `{source_label.value}` {{id: edge.source}})
            MATCH (v: `{destination_label.value}` {{id: edge.destination}})
            MERGE (u)-[:`{relationship.value}`]->(v)
            """
        parameters = {
            "edges": [
                {"source": edge.source.id_, "destination": edge.destination.id_} for edge in edges
            ]
        }
        return query_string, parameters

    def _get_embedding_dimensions(self, graph_data: _GraphData) -> int | None:
        """Embedding dimensions inferred from chunk nodes or None if it can't be determined."""
        for node in graph_data.nodes:
            if Label.CHUNK in node.labels and "embeddings" in node.properties:
                return len(node.properties["embeddings"])

        return None


neo4j_destination_entry = DestinationRegistryEntry(
    connection_config=Neo4jConnectionConfig,
    upload_stager=Neo4jUploadStager,
    upload_stager_config=Neo4jUploadStagerConfig,
    uploader=Neo4jUploader,
    uploader_config=Neo4jUploaderConfig,
)
