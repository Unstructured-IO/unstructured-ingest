from __future__ import annotations

from unstructured_ingest.processes.connector_registry import (
    add_destination_entry,
)

from .cloud import CONNECTOR_TYPE as CLOUD_CONNECTOR_TYPE
from .cloud import qdrant_cloud_destination_entry
from .local import CONNECTOR_TYPE as LOCAL_CONNECTOR_TYPE
from .local import qdrant_local_destination_entry
from .server import CONNECTOR_TYPE as SERVER_CONNECTOR_TYPE
from .server import qdrant_server_destination_entry

add_destination_entry(destination_type=CLOUD_CONNECTOR_TYPE, entry=qdrant_cloud_destination_entry)
add_destination_entry(destination_type=SERVER_CONNECTOR_TYPE, entry=qdrant_server_destination_entry)
add_destination_entry(destination_type=LOCAL_CONNECTOR_TYPE, entry=qdrant_local_destination_entry)
