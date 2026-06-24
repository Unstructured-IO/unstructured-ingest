# https://developers.notion.com/reference/block#numbered-list-item
from dataclasses import dataclass, field
from typing import Any, List, Optional

from htmlBuilder.tags import HtmlTag, Li

from unstructured_ingest.processes.connectors.notion.interfaces import BlockBase, init_from_dict
from unstructured_ingest.processes.connectors.notion.types.rich_text import RichText


@dataclass
class NumberedListItem(BlockBase):
    color: str
    children: List[dict] = field(default_factory=list)
    rich_text: List[RichText] = field(default_factory=list)
    icon: Optional[Any] = None
    # Notion may return ordered-list display metadata on the block payload.
    # list_format is one of "numbers" | "letters" | "roman"; list_start_index
    # is the first ordinal. They don't affect extracted text but must be
    # accepted so the block still maps.
    list_format: Optional[str] = None
    list_start_index: Optional[int] = None

    @staticmethod
    def can_have_children() -> bool:
        return True

    @classmethod
    def from_dict(cls, data: dict):
        rich_text = data.pop("rich_text", [])
        icon = data.pop("icon", None)
        numbered_list = init_from_dict(cls, data)
        numbered_list.rich_text = [RichText.from_dict(rt) for rt in rich_text]
        numbered_list.icon = icon
        return numbered_list

    def get_html(self) -> Optional[HtmlTag]:
        return Li([], [rt.get_html() for rt in self.rich_text])
