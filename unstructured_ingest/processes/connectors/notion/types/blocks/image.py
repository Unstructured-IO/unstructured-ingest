# https://developers.notion.com/reference/block#image
from typing import Optional

from htmlBuilder.attributes import Src
from htmlBuilder.tags import HtmlTag, Img

from unstructured_ingest.processes.connectors.notion.interfaces import BlockBase
from unstructured_ingest.processes.connectors.notion.types.file import FileObject


class Image(BlockBase, FileObject):
    @staticmethod
    def can_have_children() -> bool:
        return False

    def get_html(self) -> Optional[HtmlTag]:
        if self.external:
            return Img([Src(self.external.url)], [])
        if self.file:
            return Img([Src(self.file.url)], [])
        return None
