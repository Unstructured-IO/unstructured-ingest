from __future__ import annotations

from unstructured_ingest.processes.connector_registry import (
    add_destination_entry,
)

from .cloud import CONNECTOR_TYPE as CLOUD_WEAVIATE_CONNECTOR_TYPE
from .cloud import weaviate_cloud_destination_entry
from .embedded import CONNECTOR_TYPE as EMBEDDED_WEAVIATE_CONNECTOR_TYPE
from .embedded import weaviate_embedded_destination_entry
from .local import CONNECTOR_TYPE as LOCAL_WEAVIATE_CONNECTOR_TYPE
from .local import weaviate_local_destination_entry

add_destination_entry(
    destination_type=LOCAL_WEAVIATE_CONNECTOR_TYPE, entry=weaviate_local_destination_entry
)
add_destination_entry(
    destination_type=CLOUD_WEAVIATE_CONNECTOR_TYPE, entry=weaviate_cloud_destination_entry
)
add_destination_entry(
    destination_type=EMBEDDED_WEAVIATE_CONNECTOR_TYPE, entry=weaviate_embedded_destination_entry
)
