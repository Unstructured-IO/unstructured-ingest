# https://developers.notion.com/reference/block#table-of-contents
from dataclasses import dataclass
from typing import Any, Optional

from htmlBuilder.tags import HtmlTag

from unstructured_ingest.processes.connectors.notion.interfaces import BlockBase, init_from_dict


@dataclass
class TableOfContents(BlockBase):
    color: str
    icon: Optional[Any] = None

    @staticmethod
    def can_have_children() -> bool:
        return False

    @classmethod
    def from_dict(cls, data: dict):
        return init_from_dict(cls, data, icon=data.get("icon"))

    def get_html(self) -> Optional[HtmlTag]:
        return None
