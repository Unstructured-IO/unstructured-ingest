# https://developers.notion.com/reference/property-object#last-edited-by
from dataclasses import dataclass
from typing import Optional

from htmlBuilder.tags import HtmlTag

from unstructured_ingest.processes.connectors.notion.interfaces import DBCellBase, DBPropertyBase
from unstructured_ingest.processes.connectors.notion.types.user import People


@dataclass
class LastEditedBy(DBPropertyBase):
    @classmethod
    def from_dict(cls, data: dict):
        return cls()

    def get_text(self) -> Optional[str]:
        return None


@dataclass
class LastEditedByCell(DBCellBase):
    id: str
    last_edited_by: People
    type: str = "last_edited_by"
    description: Optional[str] = None
    name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        return cls(last_edited_by=People.from_dict(data.pop("last_edited_by", {})), **data)

    def get_html(self) -> Optional[HtmlTag]:
        return self.last_edited_by.get_html()
