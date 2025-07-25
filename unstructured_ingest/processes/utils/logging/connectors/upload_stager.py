from pathlib import Path

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.processes.utils.logging.connectors.base import ConnectorLoggingMixin


class UploadStagerConnectorLoggingMixin(ConnectorLoggingMixin):
    def log_upload_stager_start(
        self, elements_filepath: Path, file_data: FileData, output_dir: Path, output_filename: str
    ):
        self.log_operation_start(
            "Uploading stager",
            connector_type=file_data.connector_type,
            elements_filepath=elements_filepath,
            identifier=file_data.identifier,
            source_identifiers=file_data.source_identifiers,
            local_download_path=file_data.local_download_path,
            display_name=file_data.display_name,
            output_dir=output_dir,
            output_filename=output_filename,
        )

    def log_upload_stager_complete(
        self, elements_filepath: Path, file_data: FileData, output_dir: Path, output_filename: str
    ):
        self.log_operation_complete(
            "Uploading stager",
            connector_type=file_data.connector_type,
            elements_filepath=elements_filepath,
            identifier=file_data.identifier,
            source_identifiers=file_data.source_identifiers,
            local_download_path=file_data.local_download_path,
            display_name=file_data.display_name,
            output_dir=output_dir,
            output_filename=output_filename,
        )

    def log_upload_stager_failed(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        error: Exception,
    ):
        self.log_operation_failed(
            "Uploading stager",
            error,
            connector_type=file_data.connector_type,
            elements_filepath=elements_filepath,
            identifier=file_data.identifier,
            source_identifiers=file_data.source_identifiers,
            local_download_path=file_data.local_download_path,
            display_name=file_data.display_name,
            output_dir=output_dir,
            output_filename=output_filename,
        )
