from __future__ import annotations

from unstructured_ingest.v2.processes.connector_registry import (
    add_destination_entry,
)

from .postgres import CONNECTOR_TYPE as POSTGRES_CONNECTOR_TYPE
from .postgres import postgres_destination_entry
from .sqlite import CONNECTOR_TYPE as SQLITE_CONNECTOR_TYPE
from .sqlite import sqlite_destination_entry

add_destination_entry(destination_type=SQLITE_CONNECTOR_TYPE, entry=sqlite_destination_entry)
add_destination_entry(destination_type=POSTGRES_CONNECTOR_TYPE, entry=postgres_destination_entry)
