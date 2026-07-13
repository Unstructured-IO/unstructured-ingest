from typing import Optional

from unstructured_ingest.error import ValueError


def quote_identifier(name: Optional[str]) -> str:
    """Wrap ``name`` as a Databricks backtick-quoted identifier so hyphens,
    reserved words, etc. are legal; embedded backticks are doubled."""
    if not name or not name.strip():
        raise ValueError("identifier is required and cannot be empty")
    return "`" + name.replace("`", "``") + "`"


def quote_literal(value: str) -> str:
    """Wrap ``value`` as a single-quoted SQL literal, escaping backslashes then quotes.
    Databricks processes backslash escapes in literals by default, so both must be escaped."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
