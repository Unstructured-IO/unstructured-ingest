"""Unit tests for the transient-provider-error classifier in
``test/integration/embedders/utils.py``.

The classifier gates whether an embedder integration test SKIPs (the hosted
provider blinked) or FAILs (a real bug, an auth error, or quota exhaustion).
It is pure logic - no provider credentials or network access are required - so
it is exercised here as a fast unit test.
"""

import pytest

from test.integration.embedders.utils import (
    _is_transient_provider_error,
    skip_on_transient_provider,
)
from unstructured_ingest.error import (
    EmbeddingEncoderConnectionError,
    ProviderError,
    QuotaError,
    RateLimitError,
    UserAuthError,
    UserError,
)
from unstructured_ingest.error import (
    TimeoutError as UnstructuredTimeoutError,
)


class _RawStatusError(Exception):
    """Stand-in for a raw SDK exception that exposes an HTTP status code."""

    def __init__(self, status_code: int):
        super().__init__(f"status {status_code}")
        self.status_code = status_code


# Raw SDK exception classes whose *name* signals a transient condition. They
# carry no status code, so they must be caught by the name-marker fallback.
class ResourceExhausted(Exception):
    pass


class APITimeoutError(Exception):
    pass


class ServiceUnavailable(Exception):
    pass


class InternalServerError(Exception):
    pass


class _UnknownSDKError(Exception):
    """A raw SDK exception with no status code and no transient name marker."""


TRANSIENT_CASES = [
    # 1. the library's typed transient classes
    RateLimitError("rate limited"),
    ProviderError("5xx from provider"),
    UnstructuredTimeoutError("timed out"),
    EmbeddingEncoderConnectionError("connection dropped"),
    # 2. raw SDK exceptions exposing a transient HTTP status
    _RawStatusError(408),
    _RawStatusError(429),
    _RawStatusError(500),
    _RawStatusError(502),
    _RawStatusError(503),
    _RawStatusError(504),
    # 3. raw SDK exceptions matched by class-name marker
    ResourceExhausted("transient by marker"),
    APITimeoutError("timeout marker"),
    ServiceUnavailable("unavailable marker"),
    InternalServerError("5xx marker"),
]

NON_TRANSIENT_CASES = [
    UserAuthError("401 bad/expired key"),
    UserError("422 bad input"),
    QuotaError("quota exhausted - must fail, not skip"),
    AssertionError("wrong embedding output"),
    _RawStatusError(401),
    _RawStatusError(403),
    _RawStatusError(404),
    _UnknownSDKError("no status, no marker"),
]


@pytest.mark.parametrize("exc", TRANSIENT_CASES, ids=lambda e: type(e).__name__)
def test_is_transient_provider_error_true(exc: BaseException):
    assert _is_transient_provider_error(exc) is True


@pytest.mark.parametrize("exc", NON_TRANSIENT_CASES, ids=lambda e: type(e).__name__)
def test_is_transient_provider_error_false(exc: BaseException):
    assert _is_transient_provider_error(exc) is False


@pytest.mark.parametrize("exc", TRANSIENT_CASES, ids=lambda e: type(e).__name__)
def test_skip_on_transient_provider_skips(exc: BaseException):
    with pytest.raises(pytest.skip.Exception), skip_on_transient_provider("test-provider"):
        raise exc


@pytest.mark.parametrize("exc", NON_TRANSIENT_CASES, ids=lambda e: type(e).__name__)
def test_skip_on_transient_provider_reraises(exc: BaseException):
    with pytest.raises(type(exc)), skip_on_transient_provider("test-provider"):
        raise exc
