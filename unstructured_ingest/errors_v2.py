from abc import ABC
from functools import wraps


class APIError(Exception, ABC):
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


class ConnectionError(APIError):
    error_string = "Connection error: {}"


class SourceConnectionError(ConnectionError):
    error_string = "Error in getting data from upstream data source: {}"


class SourceConnectionNetworkError(SourceConnectionError):
    error_string = "Error in connecting to upstream data source: {}"


class DestinationConnectionError(ConnectionError):
    error_string = "Error in connecting to downstream data source: {}"


class EmbeddingEncoderConnectionError(ConnectionError):
    error_string = "Error in connecting to the embedding model provider: {}"


class ValueError(APIError):
    error_string = "Value error: {}"


class WriteError(APIError):
    error_string = "Error in writing to downstream data source: {}"


class PartitionError(APIError):
    error_string = "Error in partitioning content: {}"


class UserError(APIError):
    error_string = "User error: {}"


class UserAuthError(UserError):
    error_string = "User authentication error: {}"


class RateLimitError(UserError):
    error_string = "Rate limit error: {}"


class QuotaError(UserError):
    error_string = "Quota error: {}"


class ProviderError(APIError):
    error_string = "Provider error: {}"


class NotFoundError(APIError):
    error_string = "Not found error: {}"


class MissingCategoryError(APIError):
    error_string = "Missing category error: {}"


class ResponseError(APIError):
    error_string = "Response error: {}"


class ValidationError(APIError):
    error_string = "Validation error: {}"


class KeyError(APIError):
    error_string = "Key error: {}"


class FileExistsError(APIError):
    error_string = "File exists error: {}"


class TimeoutError(APIError):
    error_string = "Timeout error: {}"


class TypeError(APIError):
    error_string = "Type error: {}"


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
