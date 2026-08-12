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
    # A BARE ProviderError (no cause/context) is a contract assertion the library raises directly
    # (e.g. Vertex "successful response but no embedding") - a real defect that must FAIL, not skip,
    # despite ProviderError's default status_code of 500.
    ProviderError("contract failure: successful response but no embedding"),
]


def _provider_error_with_cause() -> ProviderError:
    """A ProviderError that explicitly wraps an underlying transient (`raise ... from e`)."""
    try:
        raise ConnectionError("underlying provider 503")
    except Exception as underlying:
        try:
            raise ProviderError("wrapped provider outage") from underlying
        except ProviderError as pe:
            return pe


def _provider_error_with_context() -> ProviderError:
    """A ProviderError raised inside an `except` block, so implicit chaining sets __context__."""
    try:
        raise ConnectionError("underlying provider 503")
    except Exception:
        try:
            raise ProviderError("wrapped provider outage")
        except ProviderError as pe:
            return pe


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


# --- ProviderError is classified by CAUSE, not by type/status ---------------
# A wrapped provider outage (raised `from e`, or from within an `except`) is transient; a bare
# contract ProviderError is not. This is the distinguishing rule that keeps a "successful response
# but no embedding" defect from being skipped while still skipping a genuine wrapped 5xx.


def test_bare_provider_error_is_not_transient():
    assert _is_transient_provider_error(ProviderError("no embedding")) is False


def test_provider_error_with_explicit_cause_is_transient():
    assert _is_transient_provider_error(_provider_error_with_cause()) is True


def test_provider_error_with_context_is_transient():
    assert _is_transient_provider_error(_provider_error_with_context()) is True


def test_bare_provider_error_fails_the_guard():
    # The contract-failure path must re-raise (fail the test), not skip.
    with pytest.raises(ProviderError), skip_on_transient_provider("test-provider"):
        raise ProviderError("successful response but no embedding")


def test_wrapped_provider_error_skips_the_guard():
    with pytest.raises(pytest.skip.Exception), skip_on_transient_provider("test-provider"):
        raise _provider_error_with_cause()
