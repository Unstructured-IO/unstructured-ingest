from typing import Optional

from unstructured_ingest.processes.utils.logging.connectors.base import ConnectorLoggingMixin


class IndexerConnectorLoggingMixin(ConnectorLoggingMixin):
    def log_indexing_start(self, connector_type: str, endpoint: Optional[str] = None):
        self.log_operation_start(
            f"Indexing files from {endpoint}",
            connector_type=connector_type,
            endpoint=endpoint,
        )

    def log_indexing_complete(
        self, connector_type: str, count: int, endpoint: Optional[str] = None
    ):
        self.log_operation_complete(
            f"Indexing {count} files from {endpoint}",
            connector_type=connector_type,
            count=count,
            endpoint=endpoint,
        )

    def log_indexing_failed(
        self, connector_type: str, error: Exception, endpoint: Optional[str] = None
    ):
        self.log_operation_failed(
            f"Indexing files from {endpoint}",
            error,
            connector_type=connector_type,
            endpoint=endpoint,
        )

    def log_indexing_progress(
        self,
        connector_type: str,
        current: int,
        total: int,
        item_type: str,
        endpoint: Optional[str] = None,
    ):
        self.log_operation_progress(
            f"Indexing files from {endpoint}",
            current=current,
            total=total,
            item_type=item_type,
            connector_type=connector_type,
            endpoint=endpoint,
        )
