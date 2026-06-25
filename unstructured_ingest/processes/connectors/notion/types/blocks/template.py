# https://developers.notion.com/reference/block#template
from dataclasses import dataclass, field
from typing import Any, List, Optional

from htmlBuilder.tags import Div, HtmlTag

from unstructured_ingest.processes.connectors.notion.interfaces import BlockBase, init_from_dict
from unstructured_ingest.processes.connectors.notion.types.rich_text import RichText


@dataclass
class Template(BlockBase):
    children: List[dict] = field(default_factory=list)
    rich_text: List[RichText] = field(default_factory=list)
    icon: Optional[Any] = None

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
        if not self.rich_text:
            return None
        return Div([], [rt.get_html() for rt in self.rich_text])
