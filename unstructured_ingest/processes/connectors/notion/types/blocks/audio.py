# https://developers.notion.com/reference/block#audio
from typing import Optional

from htmlBuilder.attributes import Src
from htmlBuilder.tags import Audio as AudioHtml
from htmlBuilder.tags import HtmlTag, Source

from unstructured_ingest.processes.connectors.notion.interfaces import BlockBase
from unstructured_ingest.processes.connectors.notion.types.file import FileObject


class Audio(BlockBase, FileObject):
    @staticmethod
    def can_have_children() -> bool:
        return False

    def get_html(self) -> Optional[HtmlTag]:
        if self.external:
            return AudioHtml([], [Source([Src(self.external.url)], [self.external.url])])
        if self.file:
            return AudioHtml([], [Source([Src(self.file.url)], [self.file.url])])
        return None
