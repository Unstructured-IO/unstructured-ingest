# https://developers.notion.com/reference/property-object#relation
from dataclasses import dataclass
from typing import Optional
from urllib.parse import unquote

from htmlBuilder.tags import Div, HtmlTag

from unstructured_ingest.processes.connectors.notion.interfaces import (
    DBCellBase,
    DBPropertyBase,
    FromJSONMixin,
    init_from_dict,
)


@dataclass
class DualProperty(FromJSONMixin):
    synced_property_id: str
    synced_property_name: str

    @classmethod
    def from_dict(cls, data: dict):
        return init_from_dict(cls, data)


@dataclass
class RelationProp(FromJSONMixin):
    database_id: str
    type: str
    # Only populated for two-way ("dual_property") relations. One-way relations
    # report type == "single_property" with an empty sub-object, so this stays
    # None for them.
    dual_property: Optional[DualProperty] = None

    @classmethod
    def from_dict(cls, data: dict):
        t = data.get("type")
        dual_property = None
        if t == "dual_property" and data.get("dual_property"):
            dual_property = DualProperty.from_dict(data["dual_property"])
        return init_from_dict(cls, data, dual_property=dual_property)


@dataclass
class Relation(DBPropertyBase):
    id: str
    name: str
    relation: RelationProp
    type: str = "relation"
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        relation = RelationProp.from_dict(data["relation"])
        return init_from_dict(cls, data, relation=relation)


@dataclass
class RelationCell(DBCellBase):
    id: str
    has_more: bool
    relation: list
    type: str = "relation"
    name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        return init_from_dict(cls, data)

    def get_html(self) -> Optional[HtmlTag]:
        return Div([], unquote(self.id))
