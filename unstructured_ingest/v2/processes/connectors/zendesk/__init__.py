from unstructured_ingest.v2.processes.connector_registry import (
    add_source_entry,
)
from .zendesk import CONNECTOR_TYPE as ZENDESK_CONNECTOR_TYPE
from .zendesk import zendesk_source_entry

add_source_entry(source_type=ZENDESK_CONNECTOR_TYPE, entry=zendesk_source_entry)