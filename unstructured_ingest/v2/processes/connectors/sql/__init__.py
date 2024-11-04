from __future__ import annotations

from unstructured_ingest.v2.processes.connector_registry import (
    add_destination_entry,
    add_source_entry,
)

from .postgres import CONNECTOR_TYPE as POSTGRES_CONNECTOR_TYPE
from .postgres import postgres_destination_entry, postgres_source_entry
from .singlestore import CONNECTOR_TYPE as SINGLESTORE_CONNECTOR_TYPE
from .singlestore import singlestore_destination_entry
from .snowflake import CONNECTOR_TYPE as SNOWFLAKE_CONNECTOR_TYPE
from .snowflake import snowflake_destination_entry, snowflake_source_entry
from .sqlite import CONNECTOR_TYPE as SQLITE_CONNECTOR_TYPE
from .sqlite import sqlite_destination_entry, sqlite_source_entry

add_source_entry(source_type=SQLITE_CONNECTOR_TYPE, entry=sqlite_source_entry)
add_source_entry(source_type=POSTGRES_CONNECTOR_TYPE, entry=postgres_source_entry)
add_source_entry(source_type=SNOWFLAKE_CONNECTOR_TYPE, entry=snowflake_source_entry)

add_destination_entry(destination_type=SQLITE_CONNECTOR_TYPE, entry=sqlite_destination_entry)
add_destination_entry(destination_type=POSTGRES_CONNECTOR_TYPE, entry=postgres_destination_entry)
add_destination_entry(destination_type=SNOWFLAKE_CONNECTOR_TYPE, entry=snowflake_destination_entry)
add_destination_entry(
    destination_type=SINGLESTORE_CONNECTOR_TYPE, entry=singlestore_destination_entry
)
