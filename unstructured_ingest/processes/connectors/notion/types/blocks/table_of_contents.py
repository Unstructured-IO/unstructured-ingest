# https://developers.notion.com/reference/block#table-of-contents
from dataclasses import dataclass
from typing import Any, Optional

from htmlBuilder.tags import HtmlTag

from unstructured_ingest.processes.connectors.notion.interfaces import BlockBase


@dataclass
class TableOfContents(BlockBase):
    color: str
    icon: Optional[Any] = None

    @staticmethod
    def can_have_children() -> bool:
        return False

    @classmethod
    def from_dict(cls, data: dict):
        icon = data.pop("icon", None)
        toc = cls(**data)
        toc.icon = icon
        return toc

    def get_html(self) -> Optional[HtmlTag]:
        return None
