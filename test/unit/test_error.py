import pytest

from unstructured_ingest.error import (
    DestinationConnectionError,
    PartitionError,
    QuotaError,
    RateLimitError,
    SourceConnectionError,
    SourceConnectionNetworkError,
    UserAuthError,
    UserError,
    safe_error_summary,
)


@pytest.mark.parametrize(
    ("error_class", "exception_type", "error_message"),
    [
        (SourceConnectionError, ValueError, "Simulated connection error"),
        (DestinationConnectionError, RuntimeError, "Simulated connection error"),
        (PartitionError, FileNotFoundError, "Simulated partition error"),
    ],
)
def test_custom_error_decorator(error_class, exception_type, error_message):
    @error_class.wrap
    def simulate_error():
        raise exception_type(error_message)

    with pytest.raises(error_class) as context:
        simulate_error()

    # Only the sanitized summary (type name plus allowlisted diagnostic
    # fields) is interpolated: original exception text can carry credentials
    # and must not leak into the wrapped message.
    expected_error_string = error_class.error_string.format(exception_type.__name__)
    assert str(context.value) == expected_error_string
    assert error_message not in str(context.value)


# Status code contract — pinned so the deliberate 401 → 422 change in PLU-377
# can't silently revert, and so the inheritance fan-out is explicit.
@pytest.mark.parametrize(
    ("error_class", "expected_status_code"),
    [
        # UserError changed from 401 to 422 in PLU-377: 422 (Unprocessable
        # Entity) is the correct HTTP semantic for "your input is invalid"
        # whereas 401 (Unauthorized) is for unauthenticated requests.
        (UserError, 422),
        # UserAuthError overrides and stays 401 — actual unauthenticated case.
        (UserAuthError, 401),
        # RateLimitError overrides with its own HTTP semantic (429).
        (RateLimitError, 429),
        # QuotaError inherits UserError without override and now reports 422
        # (was 401 before PLU-377). Pin so the inheritance contract is explicit.
        (QuotaError, 422),
    ],
)
def test_error_status_codes(error_class, expected_status_code):
    assert error_class("x").status_code == expected_status_code


# safe_error_summary contract: surface allowlisted machine-readable fields
# (integer statuses, enum-like codes, request IDs) for troubleshooting while
# never including free text, which can carry credentials or request payloads.
class FakeProviderError(Exception):
    def __init__(self, message: str, **attrs):
        super().__init__(message)
        for key, value in attrs.items():
            setattr(self, key, value)


def test_safe_error_summary_surfaces_allowlisted_fields():
    error = FakeProviderError(
        "401 for url https://api.example.com?token=sk-secret",
        status_code=401,
        code="invalid_auth",
        request_id="req-8f14e45f",
    )
    summary = safe_error_summary(error)
    assert summary == (
        "FakeProviderError(status_code=401, code=invalid_auth, request_id=req-8f14e45f)"
    )
    assert "sk-secret" not in summary


def test_safe_error_summary_rejects_free_text_fields():
    # A code-like attribute holding free text (spaces/punctuation) must be
    # dropped, not surfaced.
    error = FakeProviderError("boom", code="password=hunter2 in DSN", errno="not an int")
    assert safe_error_summary(error) == "FakeProviderError"


def test_safe_error_summary_plain_exception_is_type_name_only():
    assert safe_error_summary(ValueError("secret text")) == "ValueError"


def test_safe_error_summary_falls_back_to_response_fields():
    # slack_sdk-style: status and machine error code live on the response.
    response = {"error": "channel_not_found"}
    error = FakeProviderError("server said: {'token': 'xoxb-secret'}", response=response)
    assert safe_error_summary(error) == "FakeProviderError(error=channel_not_found)"


def test_wrap_preserves_own_error_family_guidance():
    # Our own errors carry sanitized, connector-authored guidance (e.g.
    # SharePoint's "Site not found: <site>"); wrapping into a sibling type
    # must keep that message rather than flatten it to a type-name summary.
    @SourceConnectionNetworkError.wrap
    def simulate_error():
        raise SourceConnectionError("Site not found: https://example.com/sites/team")

    with pytest.raises(SourceConnectionNetworkError) as context:
        simulate_error()

    assert "Site not found: https://example.com/sites/team" in str(context.value)


def test_wrap_message_carries_safe_fields_but_not_exception_text():
    @DestinationConnectionError.wrap
    def simulate_error():
        raise FakeProviderError("Authorization: Bearer xoxb-secret", status_code=403)

    with pytest.raises(DestinationConnectionError) as context:
        simulate_error()

    assert "FakeProviderError(status_code=403)" in str(context.value)
    assert "xoxb-secret" not in str(context.value)
