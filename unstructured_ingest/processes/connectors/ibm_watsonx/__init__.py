from __future__ import annotations

from unstructured_ingest.processes.connector_registry import add_destination_entry

from .ibm_watsonx_s3 import CONNECTOR_TYPE as IBM_WATSONX_S3_CONNECTOR_TYPE
from .ibm_watsonx_s3 import ibm_watsonx_s3_destination_entry

add_destination_entry(
    destination_type=IBM_WATSONX_S3_CONNECTOR_TYPE, entry=ibm_watsonx_s3_destination_entry
)
