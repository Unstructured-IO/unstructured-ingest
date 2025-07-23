from typing import Any, Dict, Optional

from unstructured_ingest.logger import logger
from unstructured_ingest.processes.utils.logging.sanitizer import DataSanitizer


class LoggingConfig:
    """Configuration for connector logging behavior."""

    def __init__(
        self,
        log_file_paths: bool = False,
        log_document_locations: Optional[bool] = None,
        log_ids: bool = False,
        log_document_ids: Optional[bool] = None,
        log_progress_interval: int = 10,
        sanitize_logs: bool = True,
        show_connection_details: bool = False,
    ):
        # Backward compatibility: if new parameters aren't specified, use old ones
        self.log_file_paths = log_file_paths
        self.log_document_locations = (
            log_document_locations if log_document_locations is not None else log_file_paths
        )

        self.log_ids = log_ids
        self.log_document_ids = log_document_ids if log_document_ids is not None else log_ids

        self.log_progress_interval = log_progress_interval
        self.sanitize_logs = sanitize_logs
        self.show_connection_details = show_connection_details


class ConnectorLoggingMixin:
    """Mixin class providing standardized logging patterns for connectors."""

    def __init__(self, *args, **kwargs):
        """
        Initialize the mixin by setting up logging configuration and data sanitization.

        This method ensures that the mixin provides standardized logging patterns for connectors.
        It initializes:
        - `_logging_config`: Manages logging behavior and settings.
        - `_sanitizer`: Handles sanitization of sensitive data in logs.

        Args:
            *args: Positional arguments passed to the parent class.
            **kwargs: Keyword arguments passed to the parent class.
        """
        super().__init__(*args, **kwargs)
        self._logging_config = LoggingConfig()
        self._sanitizer = DataSanitizer()

    def set_logging_config(self, config: LoggingConfig):
        """Set the logging configuration for this connector."""
        self._logging_config = config

    def _should_sanitize(self) -> bool:
        """Check if log sanitization is enabled."""
        return self._logging_config.sanitize_logs

    def log_operation_start(self, operation: str, **kwargs):
        """Log the start of a major operation."""
        logger.info("Starting %s", operation)

        if kwargs:
            if self._should_sanitize():
                sanitized_kwargs = self._sanitizer.sanitize_dict(kwargs)
                logger.debug("%s parameters: %s", operation, sanitized_kwargs)
            else:
                logger.debug("%s parameters: %s", operation, kwargs)

    def log_operation_complete(self, operation: str, count: Optional[int] = None, **kwargs):
        """Log the completion of a major operation."""
        if count is not None:
            logger.info("Completed %s (%s items)", operation, count)
        else:
            logger.info("Completed %s", operation)

        if kwargs:
            if self._should_sanitize():
                sanitized_kwargs = self._sanitizer.sanitize_dict(kwargs)
                logger.debug("%s results: %s", operation, sanitized_kwargs)
            else:
                logger.debug("%s results: %s", operation, kwargs)

    def log_connection_validated(self, connector_type: str, endpoint: Optional[str] = None):
        """Log successful connection validation."""
        if self._logging_config.show_connection_details and endpoint:
            if self._should_sanitize():
                sanitized_endpoint = self._sanitizer.sanitize_url(endpoint)
                logger.debug(
                    "Connection to %s validated successfully: %s",
                    connector_type,
                    sanitized_endpoint,
                )
            else:
                logger.debug(
                    "Connection to %s validated successfully: %s", connector_type, endpoint
                )
        else:
            logger.debug("Connection to %s validated successfully", connector_type)

    def log_connection_failed(
        self, connector_type: str, error: Exception, endpoint: Optional[str] = None
    ):
        """Log connection validation failure."""
        if endpoint:
            if self._should_sanitize():
                sanitized_endpoint = self._sanitizer.sanitize_url(endpoint)
                logger.error(
                    "Failed to validate %s connection to %s: %s",
                    connector_type,
                    sanitized_endpoint,
                    error,
                    exc_info=True,
                )
            else:
                logger.error(
                    "Failed to validate %s connection to %s: %s",
                    connector_type,
                    endpoint,
                    error,
                    exc_info=True,
                )
        else:
            logger.error(
                "Failed to validate %s connection: %s", connector_type, error, exc_info=True
            )

    def log_progress(
        self, current: int, total: int, item_type: str = "items", operation: str = "Processing"
    ):
        """Log progress for long-running operations."""
        if total > 0 and current % self._logging_config.log_progress_interval == 0:
            progress = (current / total) * 100
            logger.info("%s: %s/%s %s (%.1f%%)", operation, current, total, item_type, progress)

    def log_batch_progress(
        self, batch_num: int, total_batches: int, batch_size: int, operation: str = "Processing"
    ):
        """Log progress for batch operations."""
        logger.info("%s batch %s/%s (%s items)", operation, batch_num, total_batches, batch_size)

    def log_document_operation(
        self,
        operation: str,
        document_location: Optional[str] = None,
        document_id: Optional[str] = None,
        content_size: Optional[int] = None,
        **kwargs,
    ):
        """Log document-related operations (universal for all connector types)."""
        if self._logging_config.log_document_locations and document_location:
            if self._should_sanitize():
                sanitized_location = self._sanitizer.sanitize_location(document_location)
                logger.debug("%s: %s", operation, sanitized_location)
            else:
                logger.debug("%s: %s", operation, document_location)
        elif self._logging_config.log_document_ids and document_id:
            if self._should_sanitize():
                sanitized_id = self._sanitizer.sanitize_document_id(document_id)
                logger.debug("%s: %s", operation, sanitized_id)
            else:
                logger.debug("%s: %s", operation, document_id)
        else:
            logger.debug("%s: <document>", operation)

        if content_size is not None:
            kwargs["content_size"] = content_size

        if kwargs:
            if self._should_sanitize():
                sanitized_kwargs = self._sanitizer.sanitize_dict(kwargs)
                logger.debug("%s details: %s", operation, sanitized_kwargs)
            else:
                logger.debug("%s details: %s", operation, kwargs)

    def log_file_operation(
        self,
        operation: str,
        file_path: Optional[str] = None,
        file_id: Optional[str] = None,
        **kwargs,
    ):
        """Log file-related operations (backward compatibility wrapper)."""
        self.log_document_operation(
            operation=operation, document_location=file_path, document_id=file_id, **kwargs
        )

    def log_document_download_start(
        self,
        document_location: Optional[str] = None,
        document_id: Optional[str] = None,
        content_size: Optional[int] = None,
    ):
        """Log the start of a document download/retrieval."""
        logger.info("Starting document download")

        self.log_document_operation(
            "Download",
            document_location=document_location,
            document_id=document_id,
            content_size=content_size,
        )

    def log_document_download_complete(
        self,
        document_location: Optional[str] = None,
        document_id: Optional[str] = None,
        download_path: Optional[str] = None,
        content_size: Optional[int] = None,
        items_retrieved: Optional[int] = None,
    ):
        """Log the completion of a document download/retrieval."""
        logger.info("Document download completed")

        details = {}
        if download_path:
            details["download_path"] = download_path
        if items_retrieved is not None:
            details["items_retrieved"] = items_retrieved

        self.log_document_operation(
            "Download completed",
            document_location=document_location,
            document_id=document_id,
            content_size=content_size,
            **details,
        )

    def log_download_start(
        self,
        file_path: Optional[str] = None,
        file_id: Optional[str] = None,
        file_size: Optional[int] = None,
    ):
        """Log the start of a file download (backward compatibility wrapper)."""
        self.log_document_download_start(
            document_location=file_path, document_id=file_id, content_size=file_size
        )

    def log_download_complete(
        self,
        file_path: Optional[str] = None,
        file_id: Optional[str] = None,
        download_path: Optional[str] = None,
        file_size: Optional[int] = None,
    ):
        """Log the completion of a file download (backward compatibility wrapper)."""
        self.log_document_download_complete(
            document_location=file_path,
            document_id=file_id,
            download_path=download_path,
            content_size=file_size,
        )

    def log_upload_start(
        self,
        file_path: Optional[str] = None,
        destination: Optional[str] = None,
        file_size: Optional[int] = None,
    ):
        """Log the start of a file upload."""
        logger.info("Starting file upload")

        details = {}
        if destination:
            details["destination"] = destination

        self.log_file_operation("Upload", file_path=file_path, **details)

    def log_upload_complete(
        self,
        file_path: Optional[str] = None,
        destination: Optional[str] = None,
        file_id: Optional[str] = None,
        file_size: Optional[int] = None,
    ):
        """Log the completion of a file upload."""
        logger.info("File upload completed")

        details = {}
        if destination:
            details["destination"] = destination
        if file_id:
            details["file_id"] = file_id

        self.log_file_operation("Upload completed", file_path=file_path, **details)

    def log_indexing_start(self, source_type: str, count: Optional[int] = None):
        """Log the start of indexing operation."""
        if count:
            logger.info("Starting indexing of %s (%s items)", source_type, count)
        else:
            logger.info("Starting indexing of %s", source_type)

    def log_indexing_complete(self, source_type: str, count: int):
        """Log the completion of indexing operation."""
        logger.info("Indexing completed: %s %s items indexed", count, source_type)

    def log_info(self, message: str, context: Optional[Dict[str, Any]] = None, **kwargs):
        """Log an info message with optional context and sanitization."""
        logger.info(message)
        self._log_context("Info", context, **kwargs)

    def log_debug(self, message: str, context: Optional[Dict[str, Any]] = None, **kwargs):
        """Log a debug message with optional context and sanitization."""
        logger.debug(message)
        self._log_context("Debug", context, **kwargs)

    def log_warning(self, message: str, context: Optional[Dict[str, Any]] = None, **kwargs):
        """Log a warning message with optional context and sanitization."""
        logger.warning(message)
        self._log_context("Warning", context, **kwargs)

    def log_error(
        self,
        message: str,
        error: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """Log an error message with optional exception, context and sanitization."""
        if error:
            logger.error("%s: %s", message, error, exc_info=True)
        else:
            logger.error(message)
        self._log_context("Error", context, **kwargs)

    def _log_context(self, log_type: str, context: Optional[Dict[str, Any]], **kwargs):
        """Helper method to log context with sanitization."""
        all_context = {}
        if context:
            all_context.update(context)
        if kwargs:
            all_context.update(kwargs)

        if all_context:
            if self._should_sanitize():
                sanitized_context = self._sanitizer.sanitize_dict(all_context)
                logger.debug("%s context: %s", log_type, sanitized_context)
            else:
                logger.debug("%s context: %s", log_type, all_context)

    def log_api_call(self, method: str, endpoint: str, status_code: Optional[int] = None, **kwargs):
        """Log API call details."""
        if self._should_sanitize():
            sanitized_endpoint = self._sanitizer.sanitize_url(endpoint)
            if status_code:
                logger.debug("API call: %s %s -> %s", method, sanitized_endpoint, status_code)
            else:
                logger.debug("API call: %s %s", method, sanitized_endpoint)
        else:
            if status_code:
                logger.debug("API call: %s %s -> %s", method, endpoint, status_code)
            else:
                logger.debug("API call: %s %s", method, endpoint)

        if kwargs:
            if self._should_sanitize():
                sanitized_kwargs = self._sanitizer.sanitize_dict(kwargs)
                logger.debug("API call details: %s", sanitized_kwargs)
            else:
                logger.debug("API call details: %s", kwargs)
