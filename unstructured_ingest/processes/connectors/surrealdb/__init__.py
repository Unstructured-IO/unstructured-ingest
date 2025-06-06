from __future__ import annotations

from unstructured_ingest.processes.connector_registry import (
    add_destination_entry,
)

from .surrealdb import CONNECTOR_TYPE as SURREALDB_CONNECTOR_TYPE
from .surrealdb import surrealdb_destination_entry

add_destination_entry(destination_type=SURREALDB_CONNECTOR_TYPE, entry=surrealdb_destination_entry)
