import pytest

from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.utils.databricks import quote_identifier, quote_literal


def _unescape_databricks_literal(literal: str) -> str:
    """Mirror the Databricks/Spark lexer: strip the delimiting quotes, then process
    backslash escapes (``\\x`` -> ``x``). Databricks does NOT treat ``''`` as an escape,
    so an unescaped ``'`` in the body would terminate the literal early -- assert it never
    does, which is exactly the property quote_literal must guarantee."""
    assert literal[0] == "'" and literal[-1] == "'"
    inner = literal[1:-1]
    out: list[str] = []
    i = 0
    while i < len(inner):
        c = inner[i]
        if c == "\\" and i + 1 < len(inner):
            out.append(inner[i + 1])
            i += 2
            continue
        assert c != "'", f"unescaped quote terminates literal early: {literal!r}"
        out.append(c)
        i += 1
    return "".join(out)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("plain", "'plain'"),
        ("owner's report.pdf", "'owner\\'s report.pdf'"),
        ("o'brien's file", "'o\\'brien\\'s file'"),
        ("", "''"),
        ("''", "'\\'\\''"),
        # Backslash is doubled too -- Databricks processes backslash escapes in string
        # literals by default, so a lone backslash would escape the char that follows it.
        ("back\\slash", "'back\\\\slash'"),
        ("a\\'b", "'a\\\\\\'b'"),
        # Injection payload: a crafted \' must be neutralized so it can't escape-then-close.
        ("x\\' OR 1=1 --", "'x\\\\\\' OR 1=1 --'"),
    ],
)
def test_quote_literal_uses_backslash_escaping(value: str, expected: str):
    quoted = quote_literal(value)
    assert quoted == expected
    # The literal round-trips through the Databricks lexer back to the original value,
    # and (via the assertion inside the helper) never terminates early.
    assert _unescape_databricks_literal(quoted) == value


def test_quote_literal_neutralizes_backslash_quote_breakout():
    """A ``\\'``-crafted value must stay inside the literal: doubling the backslash turns it
    into an escaped backslash plus a backslash-escaped quote, not an escaped-quote-then-close.
    Doubling the quote instead would let ``\\'`` + ``'`` break out (the historical bug)."""
    payload = "x\\' OR 1=1 --"
    assert _unescape_databricks_literal(quote_literal(payload)) == payload


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
