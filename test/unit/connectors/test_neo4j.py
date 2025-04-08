import networkx as nx
import pytest

from test.integration.connectors.utils.constants import DESTINATION_TAG, GRAPH_DB_TAG
from unstructured_ingest.processes.connectors import neo4j
from unstructured_ingest.processes.connectors.neo4j import (
    CONNECTOR_TYPE,
    Label,
    Neo4jUploadStager,
    Relationship,
)


@pytest.mark.tags(DESTINATION_TAG, CONNECTOR_TYPE, GRAPH_DB_TAG)
def test_neo4j_stager_with_entities_no_re():
    stager = Neo4jUploadStager()

    graph = nx.MultiDiGraph()
    doc_node = neo4j._Node(id_="root", properties={"id": "root"}, labels=[Label.DOCUMENT])
    graph.add_node(doc_node)

    element_node = neo4j._Node(
        id_="element_id",
        properties={"id": "element_id", "text": "This is a test"},
        labels=[Label.UNSTRUCTURED_ELEMENT],
    )
    graph.add_edge(element_node, doc_node, relationship=Relationship.PART_OF_DOCUMENT)

    stager._add_entity_data(
        {"metadata": {"entities": [{"type": "PERSON", "entity": "Steve Jobs"}]}},
        graph,
        element_node,
    )
    assert len(graph.nodes) == 4
    assert len(graph.edges) == 3


@pytest.mark.tags(DESTINATION_TAG, CONNECTOR_TYPE, GRAPH_DB_TAG)
def test_neo4j_stager_with_entities():
    stager = Neo4jUploadStager()

    graph = nx.MultiDiGraph()
    doc_node = neo4j._Node(id_="root", properties={"id": "root"}, labels=[Label.DOCUMENT])
    graph.add_node(doc_node)

    element_node = neo4j._Node(
        id_="element_id",
        properties={"id": "element_id", "text": "This is a test"},
        labels=[Label.UNSTRUCTURED_ELEMENT],
    )
    graph.add_edge(element_node, doc_node, relationship=Relationship.PART_OF_DOCUMENT)

    stager._add_entity_data(
        {
            "metadata": {
                "entities": {
                    "items": [
                        {"type": "PERSON", "entity": "Steve Jobs"},
                        {"type": "COMPANY", "entity": "Apple"},
                    ],
                    "relationships": [
                        {"from": "Steve Jobs", "to": "Apple", "relationship": "founded"},
                        {"from": "Steve Jobs", "to": "Apple", "relationship": "worked_for"},
                    ],
                }
            }
        },
        graph,
        element_node,
    )
    assert len(graph.nodes) == 6
    assert len(graph.edges) == 7
