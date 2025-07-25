from typing import Optional

from unstructured_ingest.logger import logger
from unstructured_ingest.processes.utils.logging.sanitizer import DataSanitizer


class LoggingConfig:
    """Configuration for connector logging behavior."""

    def __init__(self, sanitize_logs: bool = True):
        self.sanitize_logs = sanitize_logs


class ConnectorLoggingMixin:
    """Mixin class providing standardized logging patterns for connectors."""

    def __init__(self):
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
        self.logging_config = LoggingConfig()
        self.sanitizer = DataSanitizer()

    def _should_sanitize(self) -> bool:
        """Check if log sanitization is enabled."""
        return self.logging_config.sanitize_logs

    def log_debug(self, message: str, **kwargs):
        """Log a debug message with optional context and sanitization."""
        logger.debug(message)
        self._log_context("Debug", **kwargs)

    def log_info(self, message: str, **kwargs):
        """Log an info message with optional context and sanitization."""
        logger.info(message)
        self._log_context("Info", **kwargs)

    def log_warning(self, message: str, **kwargs):
        """Log a warning message with optional context and sanitization."""
        logger.warning(message)
        self._log_context("Warning", **kwargs)

    def log_error(
        self,
        message: str,
        error: Optional[Exception] = None,
        **kwargs,
    ):
        """Log an error message with optional exception, context and sanitization."""
        if error:
            logger.error("%s: %s", message, error, exc_info=True)
        else:
            logger.error(message)
        self._log_context("Error", **kwargs)

    def _log_context(self, log_type: str, **kwargs):
        """Helper method to log context with sanitization."""
        if kwargs:
            if self._should_sanitize():
                sanitized_context = self.sanitizer.sanitize_dict(kwargs)
                logger.debug("%s context: %s", log_type, sanitized_context)
            else:
                logger.debug("%s context: %s", log_type, kwargs)

    def log_operation_start(self, operation: str, **kwargs):
        """Log the start of a major operation."""
        self.log_info("Starting %s", operation)

        if kwargs:
            if self._should_sanitize():
                sanitized_kwargs = self.sanitizer.sanitize_dict(kwargs)
                self.log_debug("%s parameters: %s", operation, sanitized_kwargs)
            else:
                self.log_debug("%s parameters: %s", operation, kwargs)

    def log_operation_complete(self, operation: str, **kwargs):
        """Log the completion of a major operation."""
        self.log_info("Completed %s", operation)

        if kwargs:
            if self._should_sanitize():
                sanitized_kwargs = self.sanitizer.sanitize_dict(kwargs)
                self.log_debug("%s results: %s", operation, sanitized_kwargs)
            else:
                self.log_debug("%s results: %s", operation, kwargs)

    def log_operation_failed(self, operation: str, error: Exception, **kwargs):
        """Log the failure of a major operation."""
        self.log_error("Failed %s", operation, error=error)

        if kwargs:
            if self._should_sanitize():
                sanitized_kwargs = self.sanitizer.sanitize_dict(kwargs)
                self.log_error("%s failed: %s", operation, sanitized_kwargs)
            else:
                self.log_error("%s failed: %s", operation, kwargs)

    def log_connection_validation_start(self, connector_type: str, endpoint: Optional[str] = None):
        """Log the start of a connection validation operation."""
        self.log_operation_start(
            "Connection validation", connector_type=connector_type, endpoint=endpoint
        )

    def log_connection_validation_success(
        self, connector_type: str, endpoint: Optional[str] = None
    ):
        """Log successful connection validation."""
        self.log_operation_complete(
            "Connection validation", connector_type=connector_type, endpoint=endpoint
        )

    def log_connection_validation_failed(
        self, connector_type: str, error: Exception, endpoint: Optional[str] = None
    ):
        """Log connection validation failure."""
        self.log_operation_failed(
            "Connection validation", error, connector_type=connector_type, endpoint=endpoint
        )
