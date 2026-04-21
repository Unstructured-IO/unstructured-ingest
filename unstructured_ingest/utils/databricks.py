from typing import Optional

from unstructured_ingest.error import ValueError


def quote_identifier(name: Optional[str]) -> str:
    """Wrap ``name`` as a Databricks backtick-quoted identifier so hyphens,
    reserved words, etc. are legal; embedded backticks are doubled."""
    if not name or not name.strip():
        raise ValueError("identifier is required and cannot be empty")
    return "`" + name.replace("`", "``") + "`"
