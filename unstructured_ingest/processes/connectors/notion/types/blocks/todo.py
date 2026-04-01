# https://developers.notion.com/reference/block#to-do
from dataclasses import dataclass, field
from typing import Any, List, Optional

from htmlBuilder.attributes import Checked, Style, Type
from htmlBuilder.tags import Div, HtmlTag, Input

from unstructured_ingest.processes.connectors.notion.interfaces import BlockBase
from unstructured_ingest.processes.connectors.notion.types.rich_text import RichText


@dataclass
class ToDo(BlockBase):
    color: str
    checked: bool = False
    rich_text: List[RichText] = field(default_factory=list)
    icon: Optional[Any] = None

    @staticmethod
    def can_have_children() -> bool:
        return True

    @classmethod
    def from_dict(cls, data: dict):
        rich_text = data.pop("rich_text", [])
        icon = data.pop("icon", None)
        todo = cls(**data)
        todo.rich_text = [RichText.from_dict(rt) for rt in rich_text]
        todo.icon = icon
        return todo

    def get_html(self) -> Optional[HtmlTag]:
        if not self.rich_text:
            return None

        elements = []
        check_input_attributes = [Type("checkbox")]
        if self.checked:
            check_input_attributes.append(Checked(""))
        elements.append(Input(check_input_attributes))
        elements.extend([rt.get_html() for rt in self.rich_text])
        attributes = []
        if self.color and self.color != "default":
            attributes.append(Style(f"color: {self.color}"))
        return Div(attributes, elements)
