from abc import ABC
from functools import wraps
from typing import Optional


class UnstructuredIngestError(Exception, ABC):
    error_string: str
    status_code: Optional[int] = None

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


class ConnectionError(UnstructuredIngestError):
    error_string = "Connection error: {}"
    status_code: Optional[int] = 400


class SourceConnectionError(ConnectionError):
    error_string = "Error in getting data from upstream data source: {}"
    status_code: Optional[int] = 400


class SourceConnectionNetworkError(SourceConnectionError):
    error_string = "Error in connecting to upstream data source: {}"
    status_code: Optional[int] = 400


class DestinationConnectionError(ConnectionError):
    error_string = "Error in connecting to downstream data source: {}"
    status_code: Optional[int] = 400


class EmbeddingEncoderConnectionError(ConnectionError):
    error_string = "Error in connecting to the embedding model provider: {}"
    status_code: Optional[int] = 400


class UserError(UnstructuredIngestError):
    error_string = "User error: {}"
    status_code: Optional[int] = 401


class UserAuthError(UserError):
    error_string = "User authentication error: {}"
    status_code: Optional[int] = 401


class RateLimitError(UserError):
    error_string = "Rate limit error: {}"
    status_code: Optional[int] = 429


class NotFoundError(UnstructuredIngestError):
    error_string = "Not found error: {}"
    status_code: Optional[int] = 404


class TimeoutError(UnstructuredIngestError):
    error_string = "Timeout error: {}"
    status_code: Optional[int] = 408


class ResponseError(UnstructuredIngestError):
    error_string = "Response error: {}"
    status_code: Optional[int] = 400


class WriteError(UnstructuredIngestError):
    error_string = "Error in writing to downstream data source: {}"
    status_code: Optional[int] = 400


class ProviderError(UnstructuredIngestError):
    error_string = "Provider error: {}"
    status_code: Optional[int] = 500


class ValueError(UnstructuredIngestError):
    error_string = "Value error: {}"


class PartitionError(UnstructuredIngestError):
    error_string = "Error in partitioning content: {}"


class QuotaError(UserError):
    error_string = "Quota error: {}"


class MissingCategoryError(UnstructuredIngestError):
    error_string = "Missing category error: {}"


class ValidationError(UnstructuredIngestError):
    error_string = "Validation error: {}"


class KeyError(UnstructuredIngestError):
    error_string = "Key error: {}"


class FileExistsError(UnstructuredIngestError):
    error_string = "File exists error: {}"


class TypeError(UnstructuredIngestError):
    error_string = "Type error: {}"


class IcebergCommitFailedException(UnstructuredIngestError):
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
