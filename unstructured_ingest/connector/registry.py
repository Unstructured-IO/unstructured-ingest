import json
from typing import Dict, Type, cast

from unstructured_ingest.connector.airtable import AirtableIngestDoc
from unstructured_ingest.connector.astradb import AstraDBIngestDoc
from unstructured_ingest.connector.biomed import BiomedIngestDoc
from unstructured_ingest.connector.confluence import ConfluenceIngestDoc
from unstructured_ingest.connector.delta_table import DeltaTableIngestDoc
from unstructured_ingest.connector.discord import DiscordIngestDoc
from unstructured_ingest.connector.elasticsearch import (
    ElasticsearchIngestDoc,
    ElasticsearchIngestDocBatch,
)
from unstructured_ingest.connector.fsspec.azure import AzureBlobStorageIngestDoc
from unstructured_ingest.connector.fsspec.box import BoxIngestDoc
from unstructured_ingest.connector.fsspec.dropbox import DropboxIngestDoc
from unstructured_ingest.connector.fsspec.gcs import GcsIngestDoc
from unstructured_ingest.connector.fsspec.s3 import S3IngestDoc
from unstructured_ingest.connector.fsspec.sftp import SftpIngestDoc
from unstructured_ingest.connector.github import GitHubIngestDoc
from unstructured_ingest.connector.gitlab import GitLabIngestDoc
from unstructured_ingest.connector.google_drive import GoogleDriveIngestDoc
from unstructured_ingest.connector.hubspot import HubSpotIngestDoc
from unstructured_ingest.connector.jira import JiraIngestDoc
from unstructured_ingest.connector.kafka import KafkaIngestDoc
from unstructured_ingest.connector.local import LocalIngestDoc
from unstructured_ingest.connector.mongodb import MongoDBIngestDoc, MongoDBIngestDocBatch
from unstructured_ingest.connector.notion.connector import (
    NotionDatabaseIngestDoc,
    NotionPageIngestDoc,
)
from unstructured_ingest.connector.onedrive import OneDriveIngestDoc
from unstructured_ingest.connector.opensearch import OpenSearchIngestDoc, OpenSearchIngestDocBatch
from unstructured_ingest.connector.outlook import OutlookIngestDoc
from unstructured_ingest.connector.reddit import RedditIngestDoc
from unstructured_ingest.connector.salesforce import SalesforceIngestDoc
from unstructured_ingest.connector.sharepoint import SharepointIngestDoc
from unstructured_ingest.connector.slack import SlackIngestDoc
from unstructured_ingest.connector.wikipedia import (
    WikipediaIngestHTMLDoc,
    WikipediaIngestSummaryDoc,
    WikipediaIngestTextDoc,
)
from unstructured_ingest.enhanced_dataclass import EnhancedDataClassJsonMixin
from unstructured_ingest.interfaces import BaseIngestDoc

INGEST_DOC_NAME_TO_CLASS: Dict[str, Type[EnhancedDataClassJsonMixin]] = {
    "airtable": AirtableIngestDoc,
    "astradb": AstraDBIngestDoc,
    "azure": AzureBlobStorageIngestDoc,
    "biomed": BiomedIngestDoc,
    "box": BoxIngestDoc,
    "confluence": ConfluenceIngestDoc,
    "delta-table": DeltaTableIngestDoc,
    "discord": DiscordIngestDoc,
    "dropbox": DropboxIngestDoc,
    "elasticsearch": ElasticsearchIngestDoc,
    "elasticsearch_batch": ElasticsearchIngestDocBatch,
    "gcs": GcsIngestDoc,
    "github": GitHubIngestDoc,
    "gitlab": GitLabIngestDoc,
    "google_drive": GoogleDriveIngestDoc,
    "hubspot": HubSpotIngestDoc,
    "jira": JiraIngestDoc,
    "kafka": KafkaIngestDoc,
    "local": LocalIngestDoc,
    "mongodb": MongoDBIngestDoc,
    "mongodb_batch": MongoDBIngestDocBatch,
    "notion_database": NotionDatabaseIngestDoc,
    "notion_page": NotionPageIngestDoc,
    "onedrive": OneDriveIngestDoc,
    "opensearch": OpenSearchIngestDoc,
    "opensearch_batch": OpenSearchIngestDocBatch,
    "outlook": OutlookIngestDoc,
    "reddit": RedditIngestDoc,
    "s3": S3IngestDoc,
    "salesforce": SalesforceIngestDoc,
    "sftp": SftpIngestDoc,
    "sharepoint": SharepointIngestDoc,
    "slack": SlackIngestDoc,
    "wikipedia_html": WikipediaIngestHTMLDoc,
    "wikipedia_text": WikipediaIngestTextDoc,
    "wikipedia_summary": WikipediaIngestSummaryDoc,
}


def create_ingest_doc_from_json(ingest_doc_json: str) -> BaseIngestDoc:
    try:
        ingest_doc_dict: dict = json.loads(ingest_doc_json)
    except TypeError as te:
        raise TypeError(
            f"failed to load json string when deserializing IngestDoc: {ingest_doc_json}",
        ) from te
    return create_ingest_doc_from_dict(ingest_doc_dict)


def create_ingest_doc_from_dict(ingest_doc_dict: dict) -> BaseIngestDoc:
    ingest_doc_dict = ingest_doc_dict.copy()
    if "registry_name" not in ingest_doc_dict:
        raise ValueError(f"registry_name not present in ingest doc: {ingest_doc_dict}")
    registry_name = ingest_doc_dict.pop("registry_name")
    try:
        ingest_doc_cls = INGEST_DOC_NAME_TO_CLASS[registry_name]
        return cast(BaseIngestDoc, ingest_doc_cls.from_dict(ingest_doc_dict))
    except KeyError:
        raise ValueError(
            f"Error: Received unknown IngestDoc name: {registry_name} while deserializing",
            "IngestDoc.",
        )
