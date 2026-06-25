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
        return init_from_dict(
            cls,
            data,
            rich_text=[RichText.from_dict(rt) for rt in data.get("rich_text", [])],
            icon=data.get("icon"),
        )

    def get_html(self) -> Optional[HtmlTag]:
        return Li([], [rt.get_html() for rt in self.rich_text])
