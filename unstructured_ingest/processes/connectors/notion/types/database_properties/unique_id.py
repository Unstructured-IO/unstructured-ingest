# https://developers.notion.com/reference/property-object#title
from dataclasses import dataclass, field
from typing import Optional

from htmlBuilder.tags import Div, HtmlTag

from unstructured_ingest.processes.connectors.notion.interfaces import (
    DBCellBase,
    DBPropertyBase,
    FromJSONMixin,
    init_from_dict,
)


@dataclass
class UniqueID(DBPropertyBase):
    id: str
    name: str
    type: str = "unique_id"
    unique_id: dict = field(default_factory=dict)
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        return init_from_dict(cls, data)


@dataclass
class UniqueIDCellData(FromJSONMixin):
    # Notion returns ``prefix: null`` when the column has no prefix configured.
    prefix: Optional[str]
    number: int

    @classmethod
    def from_dict(cls, data: dict):
        return init_from_dict(cls, data)


@dataclass
class UniqueIDCell(DBCellBase):
    id: str
    unique_id: Optional[UniqueIDCellData]
    type: str = "title"
    name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        return init_from_dict(
            cls, data, unique_id=UniqueIDCellData.from_dict(data["unique_id"])
        )

    def get_html(self) -> Optional[HtmlTag]:
        if unique_id := self.unique_id:
            prefix = unique_id.prefix
            text = f"{prefix}-{unique_id.number}" if prefix else str(unique_id.number)
            return Div([], text)
        return None
