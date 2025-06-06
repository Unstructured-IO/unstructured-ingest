# https://developers.notion.com/reference/block#numbered-list-item
from dataclasses import dataclass, field
from typing import List, Optional

from htmlBuilder.tags import HtmlTag, Li

from unstructured_ingest.processes.connectors.notion.interfaces import BlockBase
from unstructured_ingest.processes.connectors.notion.types.rich_text import RichText


@dataclass
class NumberedListItem(BlockBase):
    color: str
    children: List[dict] = field(default_factory=list)
    rich_text: List[RichText] = field(default_factory=list)

    @staticmethod
    def can_have_children() -> bool:
        return True

    @classmethod
    def from_dict(cls, data: dict):
        rich_text = data.pop("rich_text", [])
        numbered_list = cls(**data)
        numbered_list.rich_text = [RichText.from_dict(rt) for rt in rich_text]
        return numbered_list

    def get_html(self) -> Optional[HtmlTag]:
        return Li([], [rt.get_html() for rt in self.rich_text])
