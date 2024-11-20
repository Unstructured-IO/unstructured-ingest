from unstructured_ingest.v2.processes.connector_registry import (
    add_destination_entry,
    add_source_entry,
)

from .elasticsearch import CONNECTOR_TYPE as ELASTICSEARCH_CONNECTOR_TYPE
from .elasticsearch import elasticsearch_destination_entry, elasticsearch_source_entry
from .opensearch import CONNECTOR_TYPE as OPENSEARCH_CONNECTOR_TYPE
from .opensearch import opensearch_destination_entry, opensearch_source_entry

add_source_entry(source_type=ELASTICSEARCH_CONNECTOR_TYPE, entry=elasticsearch_source_entry)
add_destination_entry(
    destination_type=ELASTICSEARCH_CONNECTOR_TYPE, entry=elasticsearch_destination_entry
)

add_source_entry(source_type=OPENSEARCH_CONNECTOR_TYPE, entry=opensearch_source_entry)
add_destination_entry(
    destination_type=OPENSEARCH_CONNECTOR_TYPE, entry=opensearch_destination_entry
)
