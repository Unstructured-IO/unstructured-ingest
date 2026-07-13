import pytest

from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.utils.databricks import quote_identifier, quote_literal


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("plain", "'plain'"),
        ("owner's report.pdf", "'owner''s report.pdf'"),
        ("o'brien's file", "'o''brien''s file'"),
        ("", "''"),
        ("''", "''''''"),
        # Backslash must be doubled too — Databricks processes backslash escapes in string
        # literals by default, so a lone backslash would escape the char that follows it.
        ("back\\slash", "'back\\\\slash'"),
        ("a\\'b", "'a\\\\''b'"),
        # Injection payload: doubling quotes alone would let \' + ' break out of the literal.
        ("x\\' OR 1=1 --", "'x\\\\'' OR 1=1 --'"),
    ],
)
def test_quote_literal_escapes_backslashes_and_single_quotes(value: str, expected: str):
    quoted = quote_literal(value)
    assert quoted == expected
    # Balanced quotes: the literal opens and closes cleanly regardless of embedded chars.
    assert quoted.count("'") % 2 == 0


def test_quote_literal_neutralizes_backslash_quote_breakout():
    """A `\\'`-crafted value stays inside the literal: doubling the backslash makes it an
    escaped backslash plus a doubled quote, not an escaped-quote-then-close."""
    payload = "x\\' OR 1=1 --"
    inner = quote_literal(payload)[1:-1]  # strip the outer delimiting quotes
    # Simulate the server un-escaping backslash-escapes then quote-doublings.
    recovered = inner.replace("\\\\", "\\").replace("''", "'")
    assert recovered == payload


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("plain", "`plain`"),
        ("has-hyphen", "`has-hyphen`"),
        ("weird`name", "`weird``name`"),
    ],
)
def test_quote_identifier_doubles_backticks(value: str, expected: str):
    assert quote_identifier(value) == expected


@pytest.mark.parametrize("value", [None, "", "   "])
def test_quote_identifier_rejects_empty(value):
    with pytest.raises(IngestValueError):
        quote_identifier(value)
