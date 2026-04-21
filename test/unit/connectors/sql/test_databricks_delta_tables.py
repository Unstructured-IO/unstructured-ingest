"""Unit tests for databricks quote_identifier helper."""

import pytest

from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.utils.databricks import quote_identifier


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("default", "`default`"),
        ("utic-dev-tech-fixtures", "`utic-dev-tech-fixtures`"),
        ("select", "`select`"),
        ("table", "`table`"),
        ("from", "`from`"),
        ("1name", "`1name`"),
        ("my schema", "`my schema`"),
        ("café", "`café`"),
        ("foo`bar", "`foo``bar`"),
        ("a``b", "`a````b`"),
        ("`", "````"),
        ("`leading", "```leading`"),
        ("trailing`", "`trailing```"),
    ],
)
def test_quote_identifier_happy_and_escaping(raw, expected):
    assert quote_identifier(raw) == expected


@pytest.mark.parametrize(
    "bad",
    [None, "", " ", "  ", "\t", "\n", "\t \n"],
)
def test_quote_identifier_rejects_empty_and_whitespace(bad):
    with pytest.raises(IngestValueError):
        quote_identifier(bad)


def test_quote_identifier_never_uses_single_quotes():
    out = quote_identifier("utic-dev-tech-fixtures")
    assert not out.startswith("'")
    assert not out.endswith("'")
    assert out.startswith("`") and out.endswith("`")
