import re
from abc import ABC
from functools import wraps
from typing import Any, Optional

# Exception attributes that are safe to surface in logs and raised messages:
# integer HTTP statuses, SDK-defined error-code enums (SQLSTATE, errno,
# provider error codes), and opaque server-generated request/correlation IDs.
# Free-text attributes (str(e), .args, .message, response bodies, URLs,
# headers) routinely embed credentials or request payloads and are never
# surfaced.
_SAFE_ERROR_ATTRS = (
    "status_code",
    "status",
    "response_code",
    "code",
    "error_code",
    "errno",
    "sqlstate",
    "pgcode",
    "request_id",
    "sfqid",
)

# Enum-like machine codes are short tokens with no whitespace (e.g. "invalid_auth",
# "Neo.ClientError.Security.Unauthorized"). Free text virtually always
# contains whitespace or punctuation outside this set and is rejected.
#
# NOTE: this is a shape filter, not a secret detector. A credential shaped
# like a machine code (an API key "sk-live-...", an AWS key, a hex token)
# would pass it. The safety guarantee therefore rests on the
# _SAFE_ERROR_ATTRS *name* allowlist above: only attributes SDKs use for
# statuses, error codes, and request IDs are ever read. If an SDK stashed a
# secret under one of those names it would surface verbatim -- so keep
# _SAFE_ERROR_ATTRS limited to fields that never carry free text.
_MACHINE_CODE_RE = re.compile(r"[A-Za-z0-9_.\-]{1,64}")


def _safe_error_value(value: Any) -> Optional[str]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and _MACHINE_CODE_RE.fullmatch(value):
        return value
    return None


def safe_error_summary(error: BaseException) -> str:
    """Format an exception as its type name plus allowlisted diagnostic fields.

    Exception text (str(e), .args, .message, response bodies) routinely embeds
    credentials, connection strings, or request payloads, so it is never
    included. Only machine-readable fields are surfaced: integer statuses and
    error codes, enum-like code strings, and opaque request/correlation IDs,
    e.g. "SlackApiError(status_code=401, error=invalid_auth)".
    """
    details = []
    seen_status = False
    for attr in _SAFE_ERROR_ATTRS:
        try:
            value = _safe_error_value(getattr(error, attr, None))
        except Exception:
            continue
        if value is not None:
            details.append(f"{attr}={value}")
            seen_status = seen_status or attr in ("status_code", "status", "response_code")
    try:
        response = getattr(error, "response", None)
        if response is not None:
            if not seen_status:
                status = _safe_error_value(getattr(response, "status_code", None))
                if status is not None:
                    details.append(f"status_code={status}")
            # e.g. slack_sdk carries its machine error code (like
            # "channel_not_found") in the response mapping.
            getter = getattr(response, "get", None)
            if callable(getter):
                error_code = _safe_error_value(getter("error"))
                if error_code is not None:
                    details.append(f"error={error_code}")
    except Exception:
        pass
    type_name = type(error).__name__
    if not details:
        return type_name
    return f"{type_name}({', '.join(details)})"


class UnstructuredIngestError(Exception, ABC):
    error_string: str
    status_code: Optional[int] = None

    @classmethod
    def wrap(cls, f):
        """
        Provides a wrapper for a function that catches any exception and
        re-raises it as the customer error. If the exception itself is already an instance
        of the custom error, re-raises original error.

        Only the exception type name and allowlisted diagnostic fields are
        interpolated into the wrapped message; original exception text can
        carry credentials.
        """

        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except BaseException as error:
                if not isinstance(error, cls) and not issubclass(type(error), cls):
                    if isinstance(error, UnstructuredIngestError):
                        # Messages on our own error family are sanitized where
                        # they are raised; keep their guidance instead of
                        # flattening a sibling type to its summary.
                        raise cls(cls.error_string.format(error)) from error
                    raise cls(cls.error_string.format(safe_error_summary(error))) from error
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
    # 422 (Unprocessable Entity) is the correct HTTP semantic for "your input
    # is invalid". The prior 401 (Unauthorized) was wrong: 401 specifically
    # means unauthenticated, which is what UserAuthError covers below.
    error_string = "User error: {}"
    status_code: Optional[int] = 422


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
