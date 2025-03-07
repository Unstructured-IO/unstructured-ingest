from unstructured_ingest.v2.processes.connector_registry import (
    add_source_entry,
)

from .zendesk import (
    CONNECTOR_TYPE,
    ZendeskAccessConfig,
    ZendeskClient,
    ZendeskConnectionConfig,
    ZendeskDownloader,
    ZendeskDownloaderConfig,
    ZendeskIndexer,
    ZendeskIndexerConfig,
    ZendeskTicket,
    zendesk_source_entry,
)

__all__ = [
    "add_source_entry",
    "zendesk_source_entry",
    "ZendeskAccessConfig",
    "ZendeskClient",
    "ZendeskConnectionConfig",
    "ZendeskDownloader",
    "ZendeskDownloaderConfig",
    "ZendeskIndexer",
    "ZendeskIndexerConfig",
    "ZendeskTicket",
]

add_source_entry(source_type=CONNECTOR_TYPE, entry=zendesk_source_entry)
