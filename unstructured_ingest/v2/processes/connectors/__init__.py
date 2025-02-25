from __future__ import annotations

import unstructured_ingest.v2.processes.connectors.databricks  # noqa: F401
import unstructured_ingest.v2.processes.connectors.duckdb  # noqa: F401
import unstructured_ingest.v2.processes.connectors.elasticsearch  # noqa: F401
import unstructured_ingest.v2.processes.connectors.fsspec  # noqa: F401
import unstructured_ingest.v2.processes.connectors.kafka  # noqa: F401
import unstructured_ingest.v2.processes.connectors.lancedb  # noqa: F401
import unstructured_ingest.v2.processes.connectors.qdrant  # noqa: F401
import unstructured_ingest.v2.processes.connectors.sql  # noqa: F401
import unstructured_ingest.v2.processes.connectors.weaviate  # noqa: F401
from unstructured_ingest.v2.processes.connector_registry import (
    add_destination_entry,
    add_source_entry,
)

from .airtable import CONNECTOR_TYPE as AIRTABLE_CONNECTOR_TYPE
from .airtable import airtable_source_entry
from .astradb import CONNECTOR_TYPE as ASTRA_DB_CONNECTOR_TYPE
from .astradb import astra_db_destination_entry, astra_db_source_entry
from .azure_ai_search import CONNECTOR_TYPE as AZURE_AI_SEARCH_CONNECTOR_TYPE
from .azure_ai_search import azure_ai_search_destination_entry
from .chroma import CONNECTOR_TYPE as CHROMA_CONNECTOR_TYPE
from .chroma import chroma_destination_entry
from .confluence import CONNECTOR_TYPE as CONFLUENCE_CONNECTOR_TYPE
from .confluence import confluence_source_entry
from .couchbase import CONNECTOR_TYPE as COUCHBASE_CONNECTOR_TYPE
from .couchbase import couchbase_destination_entry, couchbase_source_entry
from .delta_table import CONNECTOR_TYPE as DELTA_TABLE_CONNECTOR_TYPE
from .delta_table import delta_table_destination_entry
from .discord import CONNECTOR_TYPE as DISCORD_CONNECTOR_TYPE
from .discord import discord_source_entry
from .gitlab import CONNECTOR_TYPE as GITLAB_CONNECTOR_TYPE
from .gitlab import gitlab_source_entry
from .google_drive import CONNECTOR_TYPE as GOOGLE_DRIVE_CONNECTOR_TYPE
from .google_drive import google_drive_source_entry
from .jira import CONNECTOR_TYPE as JIRA_CONNECTOR_TYPE
from .jira import jira_source_entry
from .kdbai import CONNECTOR_TYPE as KDBAI_CONNECTOR_TYPE
from .kdbai import kdbai_destination_entry
from .local import CONNECTOR_TYPE as LOCAL_CONNECTOR_TYPE
from .local import local_destination_entry, local_source_entry
from .milvus import CONNECTOR_TYPE as MILVUS_CONNECTOR_TYPE
from .milvus import milvus_destination_entry
from .mongodb import CONNECTOR_TYPE as MONGODB_CONNECTOR_TYPE
from .mongodb import mongodb_destination_entry, mongodb_source_entry
from .neo4j import CONNECTOR_TYPE as NEO4J_CONNECTOR_TYPE
from .neo4j import neo4j_destination_entry
from .notion.connector import CONNECTOR_TYPE as NOTION_CONNECTOR_TYPE
from .notion.connector import notion_source_entry
from .onedrive import CONNECTOR_TYPE as ONEDRIVE_CONNECTOR_TYPE
from .onedrive import onedrive_destination_entry, onedrive_source_entry
from .outlook import CONNECTOR_TYPE as OUTLOOK_CONNECTOR_TYPE
from .outlook import outlook_source_entry
from .pinecone import CONNECTOR_TYPE as PINECONE_CONNECTOR_TYPE
from .pinecone import pinecone_destination_entry
from .redisdb import CONNECTOR_TYPE as REDIS_CONNECTOR_TYPE
from .redisdb import redis_destination_entry
from .salesforce import CONNECTOR_TYPE as SALESFORCE_CONNECTOR_TYPE
from .salesforce import salesforce_source_entry
from .sharepoint import CONNECTOR_TYPE as SHAREPOINT_CONNECTOR_TYPE
from .sharepoint import sharepoint_source_entry
from .slack import CONNECTOR_TYPE as SLACK_CONNECTOR_TYPE
from .slack import slack_source_entry
from .vectara import CONNECTOR_TYPE as VECTARA_CONNECTOR_TYPE
from .vectara import vectara_destination_entry

add_source_entry(source_type=ASTRA_DB_CONNECTOR_TYPE, entry=astra_db_source_entry)
add_destination_entry(destination_type=ASTRA_DB_CONNECTOR_TYPE, entry=astra_db_destination_entry)

add_destination_entry(destination_type=CHROMA_CONNECTOR_TYPE, entry=chroma_destination_entry)

add_source_entry(source_type=COUCHBASE_CONNECTOR_TYPE, entry=couchbase_source_entry)
add_destination_entry(destination_type=COUCHBASE_CONNECTOR_TYPE, entry=couchbase_destination_entry)

add_destination_entry(
    destination_type=DELTA_TABLE_CONNECTOR_TYPE, entry=delta_table_destination_entry
)


add_source_entry(source_type=GOOGLE_DRIVE_CONNECTOR_TYPE, entry=google_drive_source_entry)

add_source_entry(source_type=LOCAL_CONNECTOR_TYPE, entry=local_source_entry)
add_destination_entry(destination_type=LOCAL_CONNECTOR_TYPE, entry=local_destination_entry)

add_source_entry(source_type=ONEDRIVE_CONNECTOR_TYPE, entry=onedrive_source_entry)
add_destination_entry(destination_type=ONEDRIVE_CONNECTOR_TYPE, entry=onedrive_destination_entry)

add_destination_entry(destination_type=NEO4J_CONNECTOR_TYPE, entry=neo4j_destination_entry)

add_source_entry(source_type=SALESFORCE_CONNECTOR_TYPE, entry=salesforce_source_entry)

add_destination_entry(destination_type=MONGODB_CONNECTOR_TYPE, entry=mongodb_destination_entry)
add_source_entry(source_type=MONGODB_CONNECTOR_TYPE, entry=mongodb_source_entry)

add_destination_entry(destination_type=PINECONE_CONNECTOR_TYPE, entry=pinecone_destination_entry)
add_source_entry(source_type=SHAREPOINT_CONNECTOR_TYPE, entry=sharepoint_source_entry)

add_destination_entry(destination_type=MILVUS_CONNECTOR_TYPE, entry=milvus_destination_entry)
add_destination_entry(
    destination_type=AZURE_AI_SEARCH_CONNECTOR_TYPE,
    entry=azure_ai_search_destination_entry,
)

add_destination_entry(destination_type=KDBAI_CONNECTOR_TYPE, entry=kdbai_destination_entry)
add_source_entry(source_type=AIRTABLE_CONNECTOR_TYPE, entry=airtable_source_entry)
add_source_entry(source_type=NOTION_CONNECTOR_TYPE, entry=notion_source_entry)

add_source_entry(source_type=OUTLOOK_CONNECTOR_TYPE, entry=outlook_source_entry)

add_source_entry(source_type=GITLAB_CONNECTOR_TYPE, entry=gitlab_source_entry)

add_source_entry(source_type=SLACK_CONNECTOR_TYPE, entry=slack_source_entry)

add_destination_entry(destination_type=VECTARA_CONNECTOR_TYPE, entry=vectara_destination_entry)
add_source_entry(source_type=CONFLUENCE_CONNECTOR_TYPE, entry=confluence_source_entry)

add_source_entry(source_type=DISCORD_CONNECTOR_TYPE, entry=discord_source_entry)
add_destination_entry(destination_type=REDIS_CONNECTOR_TYPE, entry=redis_destination_entry)

add_source_entry(source_type=JIRA_CONNECTOR_TYPE, entry=jira_source_entry)
