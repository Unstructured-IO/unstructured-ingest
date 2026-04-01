# https://developers.notion.com/reference/block#headings
from dataclasses import dataclass, field
from typing import Any, List, Optional

from htmlBuilder.attributes import Style
from htmlBuilder.tags import Div, HtmlTag

from unstructured_ingest.processes.connectors.notion.interfaces import BlockBase
from unstructured_ingest.processes.connectors.notion.types.rich_text import RichText


@dataclass
class Heading(BlockBase):
    color: str
    is_toggleable: bool
    rich_text: List[RichText] = field(default_factory=list)
    icon: Optional[Any] = None

    @staticmethod
    def can_have_children() -> bool:
        return False

    @classmethod
    def from_dict(cls, data: dict):
        rich_text = data.pop("rich_text", [])
        icon = data.pop("icon", None)
        heading = cls(**data)
        heading.rich_text = [RichText.from_dict(rt) for rt in rich_text]
        heading.icon = icon
        return heading

    def get_html(self) -> Optional[HtmlTag]:
        if not self.rich_text:
            return None

        texts = [rt.get_html() for rt in self.rich_text]
        attributes = []
        if self.color and self.color != "default":
            attributes.append(Style(f"color: {self.color}"))
        return Div(attributes, texts)
