from __future__ import annotations

from unstructured_ingest.v2.processes.connector_registry import (
    add_source_entry,
)

from .cloud import CONNECTOR_TYPE as CLOUD_CONNECTOR
from .cloud import kafka_cloud_source_entry
from .local import CONNECTOR_TYPE as LOCAL_CONNECTOR
from .local import kafka_local_source_entry

add_source_entry(source_type=LOCAL_CONNECTOR, entry=kafka_local_source_entry)
add_source_entry(source_type=CLOUD_CONNECTOR, entry=kafka_cloud_source_entry)
