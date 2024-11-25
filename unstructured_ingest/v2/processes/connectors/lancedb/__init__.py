from __future__ import annotations

from unstructured_ingest.v2.processes.connector_registry import add_destination_entry

from .aws import CONNECTOR_TYPE as LANCEDB_S3_CONNECTOR_TYPE
from .aws import lancedb_aws_destination_entry
from .azure import CONNECTOR_TYPE as LANCEDB_AZURE_CONNECTOR_TYPE
from .azure import lancedb_azure_destination_entry
from .cloud import CONNECTOR_TYPE as LANCEDB_CLOUD_CONNECTOR_TYPE
from .cloud import lancedb_cloud_destination_entry
from .gcp import CONNECTOR_TYPE as LANCEDB_GCS_CONNECTOR_TYPE
from .gcp import lancedb_gcp_destination_entry
from .local import CONNECTOR_TYPE as LANCEDB_LOCAL_CONNECTOR_TYPE
from .local import lancedb_local_destination_entry

add_destination_entry(
    destination_type=LANCEDB_S3_CONNECTOR_TYPE, entry=lancedb_aws_destination_entry
)
add_destination_entry(
    destination_type=LANCEDB_AZURE_CONNECTOR_TYPE, entry=lancedb_azure_destination_entry
)
add_destination_entry(
    destination_type=LANCEDB_GCS_CONNECTOR_TYPE, entry=lancedb_gcp_destination_entry
)
add_destination_entry(
    destination_type=LANCEDB_LOCAL_CONNECTOR_TYPE, entry=lancedb_local_destination_entry
)
add_destination_entry(
    destination_type=LANCEDB_CLOUD_CONNECTOR_TYPE, entry=lancedb_cloud_destination_entry
)
