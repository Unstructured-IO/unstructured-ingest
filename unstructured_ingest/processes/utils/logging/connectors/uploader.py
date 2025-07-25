from pathlib import Path

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.processes.utils.logging.connectors.base import ConnectorLoggingMixin


class UploaderConnectorLoggingMixin(ConnectorLoggingMixin):
    def log_upload_start(self, path: Path, file_data: FileData):
        self.log_operation_start(
            f"Uploading file {file_data.display_name}",
            path=path,
            identifier=file_data.identifier,
            connector_type=file_data.connector_type,
            source_identifiers=file_data.source_identifiers,
            local_download_path=file_data.local_download_path,
            display_name=file_data.display_name,
        )

    def log_upload_complete(self, path: Path, file_data: FileData):
        self.log_operation_complete(
            f"Uploading file {file_data.display_name}",
            path=path,
            identifier=file_data.identifier,
            connector_type=file_data.connector_type,
            source_identifiers=file_data.source_identifiers,
            local_download_path=file_data.local_download_path,
            display_name=file_data.display_name,
        )

    def log_upload_failed(self, path: Path, file_data: FileData, error: Exception):
        self.log_operation_failed(
            f"Uploading file {file_data.display_name}",
            error,
            path=path,
            identifier=file_data.identifier,
            connector_type=file_data.connector_type,
            source_identifiers=file_data.source_identifiers,
            local_download_path=file_data.local_download_path,
            display_name=file_data.display_name,
        )
