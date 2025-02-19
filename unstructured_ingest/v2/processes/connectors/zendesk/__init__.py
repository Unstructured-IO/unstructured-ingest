from unstructured_ingest.v2.processes.connector_registry import (
    add_source_entry,
)

from .zendesk import (
    zendesk_source_entry,
    ZendeskAccessConfig,
    ZendeskClient,
    ZendeskConnectionConfig,
    ZendeskDownloader,
    ZendeskDownloaderConfig,
    ZendeskIndexer,
    ZendeskIndexerConfig,
    ZendeskTicket
)

add_source_entry(source_type="zendesk", entry=zendesk_source_entry)
