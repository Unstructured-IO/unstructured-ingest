import json
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Optional

import pytest

from unstructured_ingest.embed.interfaces import AsyncBaseEmbeddingEncoder, BaseEmbeddingEncoder
from unstructured_ingest.error import (
    EmbeddingEncoderConnectionError,
    ProviderError,
    RateLimitError,
    safe_error_summary,
)
from unstructured_ingest.error import (
    TimeoutError as UnstructuredTimeoutError,
)

# --- Transient hosted-provider error handling -------------------------------
#
# These integration tests call real hosted embedding providers (OpenAI,
# Bedrock, Vertex, Voyage, etc.). When a provider is momentarily rate-limited,
# throttled, unavailable (5xx), or times out, the test should SKIP, not FAIL -
# the code under test is fine, the provider just blinked.
#
# Auth / misconfig errors must still FAIL. The library surfaces those as its own
# typed classes (`UserAuthError` 401, `UserError` 422, `QuotaError`) that are
# deliberately NOT in any set below, so a bad/expired key or a wrong model still
# fails loudly.
#
# The library does NOT wrap provider errors uniformly: OpenAI/Voyage/Together let
# non-SDK exceptions (timeouts, connection drops) bubble raw, Vertex wraps only
# auth errors, and Mixedbread does not wrap at all. So "transient" is detected
# three ways, strongest signal first:
#   1. the library's typed transient classes,
#   2. a transient HTTP status (429 / 408 / 5xx) read off the raw SDK exception,
#   3. a fallback match on the class name for well-known raw SDK transient
#      exceptions (google ResourceExhausted, openai APITimeoutError, ...).
#
# Failure mode is fail-safe: if none of these match, the exception re-raises and
# the test fails exactly as it does today - we never turn a real failure into a
# false pass, and none of the checks can match an auth error.

# The library's own transient error types (see unstructured_ingest/error.py).
# UserAuthError / UserError / QuotaError are intentionally excluded. ProviderError is handled
# separately (by cause, below) rather than listed here: it is a generic wrapper the library uses
# for BOTH a wrapped provider outage AND a bare contract assertion, so bare membership would
# over-skip.
_TRANSIENT_ERROR_TYPES = (
    RateLimitError,
    UnstructuredTimeoutError,
    EmbeddingEncoderConnectionError,
)

# Retry-worthy HTTP statuses: request timeout, rate limit, and server-side 5xx.
_TRANSIENT_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})

# Substrings of raw SDK exception class names that denote a transient condition.
# None of these appear in auth/misconfig exception names (Auth, Unauthorized,
# Permission, Forbidden, Credential, Denied, Validation, Quota, NotFound).
_TRANSIENT_NAME_MARKERS = (
    "RateLimit",
    "TooManyRequests",
    "Throttl",
    "ServiceUnavailable",
    "Unavailable",
    "ResourceExhausted",
    "DeadlineExceeded",
    "Timeout",
    "InternalServerError",
    "ConnectionError",
)


def _status_code_of(exc: BaseException) -> Optional[int]:
    for attr in ("status_code", "http_status"):
        value = getattr(exc, attr, None)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    response = getattr(exc, "response", None)
    if response is not None:
        value = getattr(response, "status_code", None)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def _is_transient_provider_error(exc: BaseException) -> bool:
    # Never treat an output-correctness assertion as a provider hiccup.
    if isinstance(exc, AssertionError):
        return False
    # ProviderError is a generic wrapper with a default status_code of 500, so it would otherwise
    # read as transient by BOTH type and status. Distinguish by cause: the library wraps a real
    # provider failure by raising ProviderError from within an `except` (implicit __context__) or
    # explicitly `from e` (__cause__), whereas a CONTRACT assertion (e.g. Vertex's "successful
    # response but no embedding") is raised bare, with neither set. Only the wrapped kind is a
    # transient blip; the bare kind is a real defect and must FAIL. Checked before the type/status
    # rules so the default 500 can't re-classify a bare ProviderError as transient.
    #
    # STOPGAP (FIE-365): this couples skip-behavior to HOW the error was raised, which is fragile.
    # It is exact only because, within embedder scope, the sole ProviderError is the bare contract
    # case. CONTRIBUTORS: do NOT raise a genuinely-transient ProviderError bare, and do NOT raise a
    # contract ProviderError from within an `except` -- either would be misclassified here. FIE-365
    # tracks replacing this with an explicit signal (a `transient` flag / dedicated contract type).
    if isinstance(exc, ProviderError):
        return exc.__cause__ is not None or exc.__context__ is not None
    if isinstance(exc, _TRANSIENT_ERROR_TYPES):
        return True
    status_code = _status_code_of(exc)
    if status_code in _TRANSIENT_STATUS_CODES:
        return True
    name = type(exc).__name__
    return any(marker in name for marker in _TRANSIENT_NAME_MARKERS)


@contextmanager
def skip_on_transient_provider(provider_label: str):
    """Skip (not fail) the test when the hosted provider is transiently
    unavailable / rate-limited / timing out. Auth errors and wrong-output
    assertions re-raise so they still fail."""
    try:
        yield
    except Exception as e:  # noqa: BLE001 - re-raised unless proven transient
        if _is_transient_provider_error(e):
            pytest.skip(
                f"Skipping {provider_label}: transient provider error ({safe_error_summary(e)})"
            )
        raise


def validate_embedding_output(original_elements: list[dict], output_elements: list[dict]):
    """
    Make sure the following characteristics are met:
    * The same number of elements are returned
    * For each element that had text, an embeddings entry was added in the output
    * Other than the embedding, nothing about the element was changed
    """
    assert len(original_elements) == len(output_elements)
    for original_element, output_element in zip(original_elements, output_elements):
        if original_element.get("text"):
            assert output_element.get("embeddings", None)
        output_element.pop("embeddings", None)
        assert original_element == output_element


def validate_raw_embedder(
    embedder: BaseEmbeddingEncoder,
    embedder_file: Path,
    expected_dimension: Optional[int] = None,
    expected_is_unit_vector: bool = True,
    provider_label: Optional[str] = None,
):
    with open(embedder_file) as f:
        elements = json.load(f)
    all_text = [element["text"] for element in elements]
    single_text = all_text[0]
    # provider_label is None for local encoders (huggingface) - no transient guard.
    guard = skip_on_transient_provider(provider_label) if provider_label else nullcontext()
    with guard:
        dimension = embedder.dimension
        if expected_dimension:
            assert dimension == expected_dimension, (
                f"dimensions {dimension} didn't match expected: {expected_dimension}"
            )
        is_unit_vector = embedder.is_unit_vector
        assert is_unit_vector == expected_is_unit_vector
        single_embedding = embedder.embed_query(query=single_text)
        assert len(single_embedding) == dimension
        embedded_elements = embedder.embed_documents(elements=elements)
    validate_embedding_output(original_elements=elements, output_elements=embedded_elements)


async def validate_raw_embedder_async(
    embedder: AsyncBaseEmbeddingEncoder,
    embedder_file: Path,
    expected_dimension: Optional[int] = None,
    expected_is_unit_vector: bool = True,
    provider_label: Optional[str] = None,
):
    with open(embedder_file) as f:
        elements = json.load(f)
    all_text = [element["text"] for element in elements]
    single_text = all_text[0]
    guard = skip_on_transient_provider(provider_label) if provider_label else nullcontext()
    with guard:
        dimension = await embedder.dimension
        if expected_dimension:
            assert dimension == expected_dimension, (
                f"dimension {dimension} didn't match expected: {expected_dimension}"
            )
        is_unit_vector = await embedder.is_unit_vector
        assert is_unit_vector == expected_is_unit_vector
        single_embedding = await embedder.embed_query(query=single_text)
        assert len(single_embedding) == dimension
        embedded_elements = await embedder.embed_documents(elements=elements)
    validate_embedding_output(original_elements=elements, output_elements=embedded_elements)
