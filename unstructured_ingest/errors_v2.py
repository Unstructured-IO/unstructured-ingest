from abc import ABC
from functools import wraps


class IngestError(Exception, ABC):
    error_string: str

    @classmethod
    def wrap(cls, f):
        """
        Provides a wrapper for a function that catches any exception and
        re-raises it as the customer error. If the exception itself is already an instance
        of the custom error, re-raises original error.
        """

        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except BaseException as error:
                if not isinstance(error, cls) and not issubclass(type(error), cls):
                    raise cls(cls.error_string.format(str(error))) from error
                raise

        return wrapper


class ConnectionError(IngestError):
    error_string = "Connection error: {}"


class SourceConnectionError(ConnectionError):
    error_string = "Error in getting data from upstream data source: {}"


class SourceConnectionNetworkError(SourceConnectionError):
    error_string = "Error in connecting to upstream data source: {}"


class DestinationConnectionError(ConnectionError):
    error_string = "Error in connecting to downstream data source: {}"


class EmbeddingEncoderConnectionError(ConnectionError):
    error_string = "Error in connecting to the embedding model provider: {}"


class ValueError(IngestError):
    error_string = "Value error: {}"


class WriteError(IngestError):
    error_string = "Error in writing to downstream data source: {}"


class PartitionError(IngestError):
    error_string = "Error in partitioning content: {}"


class UserError(IngestError):
    error_string = "User error: {}"


class UserAuthError(UserError):
    error_string = "User authentication error: {}"


class RateLimitError(UserError):
    error_string = "Rate limit error: {}"


class QuotaError(UserError):
    error_string = "Quota error: {}"


class ProviderError(IngestError):
    error_string = "Provider error: {}"


class NotFoundError(IngestError):
    error_string = "Not found error: {}"


class MissingCategoryError(IngestError):
    error_string = "Missing category error: {}"


class ResponseError(IngestError):
    error_string = "Response error: {}"


class ValidationError(IngestError):
    error_string = "Validation error: {}"


class KeyError(IngestError):
    error_string = "Key error: {}"


class FileExistsError(IngestError):
    error_string = "File exists error: {}"


class TimeoutError(IngestError):
    error_string = "Timeout error: {}"


class TypeError(IngestError):
    error_string = "Type error: {}"


class IcebergCommitFailedException(IngestError):
    error_string = "Failed to commit changes to the iceberg table"


recognized_errors = [
    UserError,
    UserAuthError,
    RateLimitError,
    QuotaError,
    ProviderError,
    NotFoundError,
    TypeError,
    ValueError,
    FileExistsError,
    TimeoutError,
    KeyError,
    ResponseError,
    ValidationError,
    PartitionError,
    WriteError,
    ConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
    DestinationConnectionError,
    EmbeddingEncoderConnectionError,
]


def is_internal_error(e: Exception) -> bool:
    return any(isinstance(e, recognized_error) for recognized_error in recognized_errors)
