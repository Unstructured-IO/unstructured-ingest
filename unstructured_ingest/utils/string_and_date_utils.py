import json
import re
import typing as t
from datetime import datetime

from dateutil import parser

from unstructured_ingest.v2.logger import logger


def json_to_dict(json_string: str) -> t.Union[str, t.Dict[str, t.Any]]:
    """Helper function attempts to deserialize json string to a dictionary."""
    try:
        return json.loads(json_string)
    except json.JSONDecodeError:
        # Not necessary an error if it is a path or malformed json
        pass
    try:
        # This is common when single quotes are used instead of double quotes
        return json.loads(json_string.replace("'", '"'))
    except json.JSONDecodeError:
        # Not necessary an error if it is a path
        pass
    return json_string


def ensure_isoformat_datetime(timestamp: t.Union[datetime, str]) -> str:
    """
    Ensures that the input value is converted to an ISO format datetime string.
    Handles both datetime objects and strings.
    """
    if isinstance(timestamp, datetime):
        return timestamp.isoformat()
    elif isinstance(timestamp, str):
        try:
            # Parse the datetime string in various formats
            dt = parser.parse(timestamp)
            return dt.isoformat()
        except ValueError as e:
            raise ValueError(f"String '{timestamp}' could not be parsed as a datetime.") from e
    else:
        raise TypeError(f"Expected input type datetime or str, but got {type(timestamp)}.")


def truncate_string_bytes(string: str, max_bytes: int, encoding: str = "utf-8") -> str:
    """
    Truncates a string to a specified maximum number of bytes.
    """
    encoded_string = str(string).encode(encoding)
    if len(encoded_string) <= max_bytes:
        return string
    return encoded_string[:max_bytes].decode(encoding, errors="ignore")


def fix_unescaped_unicode(text: str, encoding: str = "utf-8") -> str:
    """
    Fix unescaped Unicode sequences in text.
    """
    try:
        _text: str = json.dumps(text)

        # Pattern to match unescaped Unicode sequences like \\uXXXX
        pattern = r"\\\\u([0-9A-Fa-f]{4})"
        # Replace with properly escaped Unicode sequences \uXXXX
        _text = re.sub(pattern, r"\\u\1", _text)
        _text = json.loads(_text)

        # Encode the text to check for encoding errors
        _text.encode(encoding)
        return _text
    except Exception as e:
        # Return original text if encoding fails
        logger.warning(f"Failed to fix unescaped Unicode sequences: {e}", exc_info=True)
        return text
