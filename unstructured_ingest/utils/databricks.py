from typing import Optional

from unstructured_ingest.error import ValueError


def quote_identifier(name: Optional[str]) -> str:
    """Wrap ``name`` as a Databricks backtick-quoted identifier so hyphens,
    reserved words, etc. are legal; embedded backticks are doubled."""
    if not name or not name.strip():
        raise ValueError("identifier is required and cannot be empty")
    return "`" + name.replace("`", "``") + "`"


def quote_literal(value: str) -> str:
    """Wrap ``value`` as a single-quoted SQL literal using Databricks' escape syntax.

    Databricks/Spark uses backslash escaping in string literals
    (``spark.sql.parser.escapedStringLiterals`` is ``false`` by default) and does NOT
    recognize the ANSI doubled-quote (``''``) escape below Spark master -- ``'O''Connell'``
    lexes as the two literals ``'O'`` and ``'Connell'``, a syntax error (SQLSTATE 42601).
    So escape backslashes first (so a source backslash can't escape the char that follows
    it) and then single quotes as ``\\'``. See the Databricks STRING type docs."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
