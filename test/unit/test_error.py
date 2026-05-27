import pytest

from unstructured_ingest.error import (
    DestinationConnectionError,
    PartitionError,
    QuotaError,
    RateLimitError,
    SourceConnectionError,
    UserAuthError,
    UserError,
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

    expected_error_string = error_class.error_string.format(error_message)
    assert str(context.value) == expected_error_string


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
