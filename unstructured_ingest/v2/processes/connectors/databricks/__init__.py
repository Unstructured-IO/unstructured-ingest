from __future__ import annotations

from unstructured_ingest.v2.processes.connector_registry import (
    add_destination_entry,
    add_source_entry,
)

from .volumes_aws import CONNECTOR_TYPE as VOLUMES_AWS_CONNECTOR_TYPE
from .volumes_aws import (
    databricks_aws_volumes_destination_entry,
    databricks_aws_volumes_source_entry,
)
from .volumes_azure import CONNECTOR_TYPE as VOLUMES_AZURE_CONNECTOR_TYPE
from .volumes_azure import (
    databricks_azure_volumes_destination_entry,
    databricks_azure_volumes_source_entry,
)
from .volumes_gcp import CONNECTOR_TYPE as VOLUMES_GCP_CONNECTOR_TYPE
from .volumes_gcp import (
    databricks_gcp_volumes_destination_entry,
    databricks_gcp_volumes_source_entry,
)
from .volumes_native import CONNECTOR_TYPE as VOLUMES_NATIVE_CONNECTOR_TYPE
from .volumes_native import (
    databricks_native_volumes_destination_entry,
    databricks_native_volumes_source_entry,
)
from .volumes_table import CONNECTOR_TYPE as VOLUMES_TABLE_CONNECTOR_TYPE
from .volumes_table import databricks_volumes_delta_tables_destination_entry

add_source_entry(source_type=VOLUMES_AWS_CONNECTOR_TYPE, entry=databricks_aws_volumes_source_entry)
add_destination_entry(
    destination_type=VOLUMES_AWS_CONNECTOR_TYPE, entry=databricks_aws_volumes_destination_entry
)

add_source_entry(source_type=VOLUMES_GCP_CONNECTOR_TYPE, entry=databricks_gcp_volumes_source_entry)
add_destination_entry(
    destination_type=VOLUMES_GCP_CONNECTOR_TYPE, entry=databricks_gcp_volumes_destination_entry
)

add_source_entry(
    source_type=VOLUMES_NATIVE_CONNECTOR_TYPE, entry=databricks_native_volumes_source_entry
)
add_destination_entry(
    destination_type=VOLUMES_NATIVE_CONNECTOR_TYPE,
    entry=databricks_native_volumes_destination_entry,
)

add_source_entry(
    source_type=VOLUMES_AZURE_CONNECTOR_TYPE, entry=databricks_azure_volumes_source_entry
)
add_destination_entry(
    destination_type=VOLUMES_AZURE_CONNECTOR_TYPE, entry=databricks_azure_volumes_destination_entry
)
add_destination_entry(
    destination_type=VOLUMES_TABLE_CONNECTOR_TYPE,
    entry=databricks_volumes_delta_tables_destination_entry,
)
