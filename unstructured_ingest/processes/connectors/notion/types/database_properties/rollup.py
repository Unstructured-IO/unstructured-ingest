# https://developers.notion.com/reference/property-object#rollup
from dataclasses import dataclass
from typing import Optional

from htmlBuilder.tags import Div, HtmlTag, Span

from unstructured_ingest.processes.connectors.notion.interfaces import (
    DBCellBase,
    DBPropertyBase,
    FromJSONMixin,
    init_from_dict,
)


@dataclass
class RollupProp(FromJSONMixin):
    function: str
    relation_property_id: str
    relation_property_name: str
    rollup_property_id: str
    rollup_property_name: str

    @classmethod
    def from_dict(cls, data: dict):
        return init_from_dict(cls, data)


@dataclass
class Rollup(DBPropertyBase):
    id: str
    name: str
    rollup: RollupProp
    type: str = "rollup"
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        return init_from_dict(cls, data, rollup=RollupProp.from_dict(data["rollup"]))


def _value_to_text(value_type: Optional[str], value) -> str:
    """Best-effort plain text for a Notion property value.

    Rollup cells embed raw property-value objects (e.g. an ``array`` rollup of a
    title property yields ``{"type": "title", "title": [<rich text>...]}``).
    Rendering these with ``str()`` leaked the raw Python dict into the output, so
    extract readable text per value type and fall back to an empty string for
    anything unmodeled rather than dumping the object.
    """
    if value is None:
        return ""
    if value_type in ("title", "rich_text"):
        if isinstance(value, list):
            return "".join(rt.get("plain_text", "") for rt in value if isinstance(rt, dict))
        return ""
    if value_type in ("select", "status"):
        return value.get("name", "") if isinstance(value, dict) else ""
    if value_type == "multi_select":
        if isinstance(value, list):
            return ", ".join(o.get("name", "") for o in value if isinstance(o, dict))
        return ""
    if value_type == "people":
        if isinstance(value, list):
            return ", ".join(p.get("name", "") for p in value if isinstance(p, dict))
        return ""
    if value_type == "date":
        if isinstance(value, dict):
            start = value.get("start") or ""
            end = value.get("end")
            return f"{start} - {end}" if end else start
        return ""
    if value_type == "formula":
        if isinstance(value, dict):
            inner = value.get("type")
            return _value_to_text(inner, value.get(inner))
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (str, int, float)):
        return str(value)
    # Unknown/object value: never dump a raw dict.
    return ""


def _rollup_item_to_text(item) -> str:
    if isinstance(item, dict):
        item_type = item.get("type")
        return _value_to_text(item_type, item.get(item_type))
    return str(item)


@dataclass
class RollupCell(DBCellBase):
    id: str
    rollup: dict
    type: str = "rollup"
    name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        return init_from_dict(cls, data)

    def get_html(self) -> Optional[HtmlTag]:
        rollup = self.rollup or {}
        rollup_type = rollup.get("type")
        if rollup_type == "array":
            items = rollup.get("array") or []
            spans = [Span([], text) for text in map(_rollup_item_to_text, items) if text]
            return Div([], spans) if spans else None
        text = _value_to_text(rollup_type, rollup.get(rollup_type))
        return Div([], text) if text else None
