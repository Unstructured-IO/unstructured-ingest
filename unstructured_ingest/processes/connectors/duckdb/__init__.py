from __future__ import annotations

from unstructured_ingest.processes.connector_registry import (
    add_destination_entry,
)

from .duckdb import CONNECTOR_TYPE as DUCKDB_CONNECTOR_TYPE
from .duckdb import duckdb_destination_entry
from .motherduck import CONNECTOR_TYPE as MOTHERDUCK_CONNECTOR_TYPE
from .motherduck import motherduck_destination_entry

add_destination_entry(destination_type=DUCKDB_CONNECTOR_TYPE, entry=duckdb_destination_entry)
add_destination_entry(
    destination_type=MOTHERDUCK_CONNECTOR_TYPE, entry=motherduck_destination_entry
)
