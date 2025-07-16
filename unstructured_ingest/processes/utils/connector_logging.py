from pathlib import Path
from time import time
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

from unstructured_ingest.logger import logger


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
        log_timings: bool = True,
        timing_threshold_seconds: float = 1.0,
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
        self.log_timings = log_timings
        self.timing_threshold_seconds = timing_threshold_seconds


class OperationTimer:
    """Utility class for tracking operation timing."""

    def __init__(self):
        self.start_times: Dict[str, float] = {}
        self.durations: Dict[str, list[float]] = {}

    def start_operation(self, operation_name: str) -> None:
        """Start timing an operation."""
        self.start_times[operation_name] = time()

    def end_operation(self, operation_name: str) -> Optional[float]:
        """End timing an operation and return duration in seconds."""
        if operation_name not in self.start_times:
            return None

        duration = time() - self.start_times[operation_name]
        del self.start_times[operation_name]

        if operation_name not in self.durations:
            self.durations[operation_name] = []
        self.durations[operation_name].append(duration)

        return duration

    def get_statistics(self, operation_name: str) -> Optional[Dict[str, Any]]:
        """Get timing statistics for an operation."""
        if operation_name not in self.durations or not self.durations[operation_name]:
            return None

        durations = self.durations[operation_name]
        return {
            "count": len(durations),
            "total_seconds": sum(durations),
            "average_seconds": sum(durations) / len(durations),
            "min_seconds": min(durations),
            "max_seconds": max(durations),
        }

    def format_duration(self, seconds: float) -> str:
        """Format duration in a human-readable way."""
        if seconds < 1:
            return f"{seconds * 1000:.1f}ms"
        elif seconds < 60:
            return f"{seconds:.2f}s"
        elif seconds < 3600:
            return f"{seconds / 60:.1f}m"
        else:
            return f"{seconds / 3600:.1f}h"

    def get_active_operations(self) -> list[str]:
        """Get list of currently active (started but not ended) operations."""
        return list(self.start_times.keys())


class DataSanitizer:
    """Utility class for sanitizing sensitive data in logs."""

    @staticmethod
    def sanitize_path(path: Union[str, Path]) -> str:
        """Sanitize file paths for logging, showing only filename and partial path."""
        if not path:
            return "<empty>"

        path_str = str(path)
        path_obj = Path(path_str)

        if len(path_obj.parts) > 2:
            return f".../{path_obj.parent.name}/{path_obj.name}"
        return path_obj.name

    @staticmethod
    def sanitize_id(identifier: str) -> str:
        """Sanitize IDs for logging, showing only first/last few characters."""
        if not identifier or len(identifier) < 8:
            return "<id>"
        return f"{identifier[:4]}...{identifier[-4:]}"

    @staticmethod
    def sanitize_url(url: str) -> str:
        """Sanitize URLs for logging, removing sensitive query parameters."""
        if not url:
            return "<url>"
        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        except Exception:
            return "<url>"

    @staticmethod
    def sanitize_token(token: str) -> str:
        """Sanitize tokens and secrets for logging."""
        if not token:
            return "<token>"
        if len(token) < 8:
            return "<token>"
        return f"{token[:4]}...{token[-4:]}"

    @staticmethod
    def sanitize_location(location: Union[str, Path]) -> str:
        """Sanitize document locations (file paths, URLs, database references) for logging."""
        if not location:
            return "<empty>"

        location_str = str(location)

        # Handle URLs
        if location_str.startswith(("http://", "https://", "ftp://", "ftps://")):
            return DataSanitizer.sanitize_url(location_str)

        # Handle database-style references (table:id, collection/document, etc.)
        if ":" in location_str and not location_str.startswith("/"):
            parts = location_str.split(":", 1)
            if len(parts) == 2:
                table_name, record_id = parts
                return f"{table_name}:{DataSanitizer.sanitize_id(record_id)}"

        return DataSanitizer.sanitize_path(location_str)

    @staticmethod
    def sanitize_document_id(document_id: str) -> str:
        """Sanitize document IDs for logging (alias for sanitize_id for clarity)."""
        return DataSanitizer.sanitize_id(document_id)

    @staticmethod
    def sanitize_dict(data: Dict[str, Any], sensitive_keys: Optional[set] = None) -> Dict[str, Any]:
        """Sanitize dictionary data for logging."""
        if sensitive_keys is None:
            sensitive_keys = {
                "password",
                "token",
                "secret",
                "key",
                "api_key",
                "access_token",
                "refresh_token",
                "client_secret",
                "private_key",
                "credentials",
            }

        sanitized = {}
        for k, v in data.items():
            key_lower = k.lower()
            if any(sensitive_key in key_lower for sensitive_key in sensitive_keys):
                sanitized[k] = DataSanitizer.sanitize_token(str(v))
            elif isinstance(v, dict):
                sanitized[k] = DataSanitizer.sanitize_dict(v, sensitive_keys)
            elif isinstance(v, (str, Path)) and (
                "path" in key_lower
                or "file" in key_lower
                or "location" in key_lower
                or "document_location" in key_lower
            ):
                sanitized[k] = DataSanitizer.sanitize_location(v)
            elif isinstance(v, str) and (
                ("id" in key_lower and len(str(v)) > 8)
                or ("document_id" in key_lower and len(str(v)) > 8)
            ):
                sanitized[k] = DataSanitizer.sanitize_document_id(v)
            else:
                sanitized[k] = v
        return sanitized


class ConnectorLoggingMixin:
    """Mixin class providing standardized logging patterns for connectors."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logging_config = LoggingConfig()
        self._sanitizer = DataSanitizer()
        self._timer = OperationTimer()

    def set_logging_config(self, config: LoggingConfig):
        """Set the logging configuration for this connector."""
        self._logging_config = config

    def _should_sanitize(self) -> bool:
        """Check if log sanitization is enabled."""
        return self._logging_config.sanitize_logs

    def _should_log_timing(self) -> bool:
        """Check if timing logging is enabled."""
        return self._logging_config.log_timings

    def _log_timing(self, operation: str, duration: float, **kwargs):
        """Log timing information if enabled and above threshold."""
        if not self._should_log_timing():
            return

        if duration >= self._logging_config.timing_threshold_seconds:
            formatted_duration = self._timer.format_duration(duration)
            logger.info("%s completed in %s", operation, formatted_duration)

            if kwargs:
                if self._should_sanitize():
                    sanitized_kwargs = self._sanitizer.sanitize_dict(kwargs)
                    logger.debug("Timing details: %s", sanitized_kwargs)
                else:
                    logger.debug("Timing details: %s", kwargs)

    def log_operation_start(self, operation: str, **kwargs):
        """Log the start of a major operation and start timing."""
        logger.info("Starting %s", operation)

        self._timer.start_operation(operation)

        if kwargs:
            if self._should_sanitize():
                sanitized_kwargs = self._sanitizer.sanitize_dict(kwargs)
                logger.debug("%s parameters: %s", operation, sanitized_kwargs)
            else:
                logger.debug("%s parameters: %s", operation, kwargs)

    def log_operation_complete(self, operation: str, count: Optional[int] = None, **kwargs):
        """Log the completion of a major operation and log timing."""
        duration = self._timer.end_operation(operation)

        if count is not None:
            logger.info("Completed %s (%s items)", operation, count)
        else:
            logger.info("Completed %s", operation)

        if duration is not None:
            timing_kwargs = dict(kwargs)
            if count is not None:
                timing_kwargs["items_processed"] = count
                timing_kwargs["items_per_second"] = (
                    round(count / duration, 2) if duration > 0 else 0
                )
            self._log_timing(operation, duration, **timing_kwargs)

        if kwargs:
            if self._should_sanitize():
                sanitized_kwargs = self._sanitizer.sanitize_dict(kwargs)
                logger.debug("%s results: %s", operation, sanitized_kwargs)
            else:
                logger.debug("%s results: %s", operation, kwargs)

    def log_timed_operation(self, operation: str, duration: float, **kwargs):
        """Log a pre-timed operation (when you measure duration externally)."""
        self._log_timing(operation, duration, **kwargs)

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
        """Log the start of a document download/retrieval and start timing."""
        operation_name = f"Download {document_id or 'document'}"

        if content_size:
            logger.info("Starting document download (%s bytes)", content_size)
        else:
            logger.info("Starting document download")

        self._timer.start_operation(operation_name)

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
        """Log the completion of a document download/retrieval and log timing."""
        operation_name = f"Download {document_id or 'document'}"

        duration = self._timer.end_operation(operation_name)

        logger.info("Document download completed")

        details = {}
        if download_path:
            details["download_path"] = download_path
        if items_retrieved is not None:
            details["items_retrieved"] = items_retrieved

        if duration is not None:
            if content_size:
                transfer_speed = content_size / duration if duration > 0 else 0
                details["transfer_speed_mbps"] = round(transfer_speed / (1024 * 1024), 2)
            elif items_retrieved:
                processing_speed = items_retrieved / duration if duration > 0 else 0
                details["items_per_second"] = round(processing_speed, 2)
            self._log_timing(operation_name, duration, **details)

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
        """Log the start of a file download and start timing (backward compatibility wrapper)."""
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
        """Log the completion of a file download and log timing (backward compatibility wrapper)."""
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
        """Log the start of a file upload and start timing."""
        operation_name = f"Upload {Path(file_path).name if file_path else 'file'}"

        if file_size:
            logger.info("Starting file upload (%s bytes)", file_size)
        else:
            logger.info("Starting file upload")

        self._timer.start_operation(operation_name)

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
        """Log the completion of a file upload and log timing."""
        operation_name = f"Upload {Path(file_path).name if file_path else 'file'}"

        duration = self._timer.end_operation(operation_name)

        logger.info("File upload completed")

        details = {}
        if destination:
            details["destination"] = destination
        if file_id:
            details["file_id"] = file_id

        if duration is not None and file_size:
            transfer_speed = file_size / duration if duration > 0 else 0
            details["transfer_speed_mbps"] = round(transfer_speed / (1024 * 1024), 2)
            self._log_timing(operation_name, duration, **details)

        self.log_file_operation("Upload completed", file_path=file_path, **details)

    def log_indexing_start(self, source_type: str, count: Optional[int] = None):
        """Log the start of indexing operation and start timing."""
        operation_name = f"Indexing {source_type}"

        if count:
            logger.info("Starting indexing of %s (%s items)", source_type, count)
        else:
            logger.info("Starting indexing of %s", source_type)

        self._timer.start_operation(operation_name)

    def log_indexing_complete(self, source_type: str, count: int):
        """Log the completion of indexing operation and log timing."""
        operation_name = f"Indexing {source_type}"

        duration = self._timer.end_operation(operation_name)

        logger.info("Indexing completed: %s %s items indexed", count, source_type)

        if duration is not None:
            indexing_speed = count / duration if duration > 0 else 0
            self._log_timing(
                operation_name,
                duration,
                items_indexed=count,
                items_per_second=round(indexing_speed, 2),
            )

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

    def log_warning_with_context(self, message: str, context: Optional[Dict[str, Any]] = None):
        """Log a warning with optional context."""
        logger.warning(message)
        if context:
            if self._should_sanitize():
                sanitized_context = self._sanitizer.sanitize_dict(context)
                logger.debug("Warning context: %s", sanitized_context)
            else:
                logger.debug("Warning context: %s", context)

    def log_error_with_context(
        self, message: str, error: Exception, context: Optional[Dict[str, Any]] = None
    ):
        """Log an error with optional context."""
        logger.error("%s: %s", message, error, exc_info=True)
        if context:
            if self._should_sanitize():
                sanitized_context = self._sanitizer.sanitize_dict(context)
                logger.debug("Error context: %s", sanitized_context)
            else:
                logger.debug("Error context: %s", context)

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

    def log_summary(self, operation: str, stats: Dict[str, Any]):
        """Log a summary of operations with statistics."""
        logger.info("%s summary:", operation)
        for key, value in stats.items():
            logger.info("  %s: %s", key, value)

    def log_timing_summary(self, operation: str):
        """Log timing statistics for an operation."""
        stats = self._timer.get_statistics(operation)
        if stats:
            logger.info("%s timing summary:", operation)
            logger.info("Total time: %s", self._timer.format_duration(stats["total_seconds"]))
            logger.info("Average time: %s", self._timer.format_duration(stats["average_seconds"]))
            logger.info("Min time: %s", self._timer.format_duration(stats["min_seconds"]))
            logger.info("Max time: %s", self._timer.format_duration(stats["max_seconds"]))
            logger.info("Operation count: %s", stats["count"])

    def get_active_operations(self) -> list[str]:
        """Get list of currently active operations (for debugging)."""
        return self._timer.get_active_operations()

    def get_operation_statistics(self, operation: str) -> Optional[Dict[str, Any]]:
        """Get timing statistics for a specific operation."""
        return self._timer.get_statistics(operation)
