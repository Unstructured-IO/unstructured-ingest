import json
from datetime import datetime
from typing import Any, Union

from dateutil import parser
from pydantic import ValidationError

from unstructured_ingest.utils.chunking import elements_from_base64_gzipped_json


def parse_datetime(date_value: Union[int, str, float, datetime]) -> datetime:
    if isinstance(date_value, datetime):
        return date_value
    elif isinstance(date_value, float):
        return datetime.fromtimestamp(date_value)
    elif isinstance(date_value, int):
        return datetime.fromtimestamp(date_value / 1000)

    try:
        timestamp = float(date_value)
        return datetime.fromtimestamp(timestamp)
    except ValueError:
        return parser.parse(date_value)


def conform_string_to_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    raise ValidationError(f"Input could not be mapped to a valid dict: {value}")


def format_and_truncate_orig_elements(
    element: dict, include_text: bool = False
) -> list[dict[str, Any]]:
    """
    This function is used to format and truncate the orig_elements field in the metadata.
    This is used to remove the text field and other larger fields from the orig_elements
    that are not helpful in filtering/searching when used along with chunked elements.
    """
    metadata = element.get("metadata", {})
    raw_orig_elements = metadata.get("orig_elements", None)
    orig_elements = []
    if raw_orig_elements is not None:
        for element in elements_from_base64_gzipped_json(raw_orig_elements):
            if not include_text:
                element.pop("text", None)
            for prop in (
                "image_base64",
                "text_as_html",
                "table_as_cells",
                "link_urls",
                "link_texts",
                "link_start_indexes",
                "emphasized_text_contents",
            ):
                element["metadata"].pop(prop, None)
            orig_elements.append(element)
    return orig_elements
