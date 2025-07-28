from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.processes.utils.logging.connectors.base import ConnectorLoggingMixin


class DownloaderConnectorLoggingMixin(ConnectorLoggingMixin):
    def log_download_start(self, file_data: FileData):
        self.log_operation_start(
            f"Downloading file {file_data.display_name}",
            identifier=file_data.identifier,
            connector_type=file_data.connector_type,
            source_identifiers=file_data.source_identifiers,
            local_download_path=file_data.local_download_path,
            display_name=file_data.display_name,
        )

    def log_download_complete(self, file_data: FileData):
        self.log_operation_complete(
            f"Downloading file {file_data.display_name}",
            identifier=file_data.identifier,
            connector_type=file_data.connector_type,
            source_identifiers=file_data.source_identifiers,
            local_download_path=file_data.local_download_path,
            display_name=file_data.display_name,
        )

    def log_download_failed(self, file_data: FileData, error: Exception):
        self.log_operation_failed(
            f"Downloading file {file_data.display_name}",
            error,
            identifier=file_data.identifier,
            connector_type=file_data.connector_type,
            source_identifiers=file_data.source_identifiers,
            local_download_path=file_data.local_download_path,
            display_name=file_data.display_name,
        )
