# https://developers.notion.com/reference/block#synced-block
from dataclasses import dataclass, field
from typing import List, Optional

from htmlBuilder.tags import HtmlTag

from unstructured_ingest.processes.connectors.notion.interfaces import BlockBase


@dataclass
class OriginalSyncedBlock(BlockBase):
    synced_from: Optional[str] = None
    children: List[dict] = field(default_factory=list)

    @staticmethod
    def can_have_children() -> bool:
        return True

    @classmethod
    def from_dict(cls, data: dict):
        """Create OriginalSyncedBlock from dictionary data.

        Original blocks contain children content.
        """
        if "children" not in data:
            raise ValueError(f"OriginalSyncedBlock data missing 'children': {data}")
        return cls(children=data["children"])

    def get_html(self) -> Optional[HtmlTag]:
        return None


@dataclass
class DuplicateSyncedBlock(BlockBase):
    type: str
    block_id: str

    @staticmethod
    def can_have_children() -> bool:
        """Check if duplicate synced blocks can have children.

        Duplicate blocks themselves don't have children directly fetched here,
        but they represent content that does, so Notion API might report has_children=True
        on the parent block object. The actual children are fetched from the original block.
        """
        return True

    @classmethod
    def from_dict(cls, data: dict):
        """Create DuplicateSyncedBlock from dictionary data.

        Duplicate blocks contain a 'synced_from' reference.
        """
        synced_from_data = data.get("synced_from")
        if not synced_from_data or not isinstance(synced_from_data, dict):
            raise ValueError(f"Invalid data structure for DuplicateSyncedBlock: {data}")
        # Ensure required keys are present in the nested dictionary
        if "type" not in synced_from_data or "block_id" not in synced_from_data:
            raise ValueError(
                f"Missing 'type' or 'block_id' in synced_from data: {synced_from_data}"
            )
        return cls(type=synced_from_data["type"], block_id=synced_from_data["block_id"])

    def get_html(self) -> Optional[HtmlTag]:
        """Get HTML representation of the duplicate synced block.

        HTML representation might need fetching the original block's content,
        which is outside the scope of this simple data class.
        """
        return None


class SyncBlock(BlockBase):
    @staticmethod
    def can_have_children() -> bool:
        """Check if synced blocks can have children.

        Synced blocks (both original and duplicate) can conceptually have children.
        """
        return True

    @classmethod
    def from_dict(cls, data: dict):
        """Create appropriate SyncedBlock subclass from dictionary data.

        Determine if it's a duplicate (has 'synced_from') or original (has 'children').
        """
        if data.get("synced_from") is not None:
            # It's a duplicate block containing a reference
            return DuplicateSyncedBlock.from_dict(data)
        elif "children" in data:
            # It's an original block containing children
            return OriginalSyncedBlock.from_dict(data)
        else:
            # Handle cases where neither 'synced_from' nor 'children' are present.
            # Notion API might return this for an empty original synced block.
            # Let's treat it as an empty OriginalSyncedBlock.
            # If this assumption is wrong, errors might occur later.
            # Consider logging a warning here if strictness is needed.
            return OriginalSyncedBlock(children=[])

    def get_html(self) -> Optional[HtmlTag]:
        """Get HTML representation of the synced block.

        The specific instance returned by from_dict (Original or Duplicate)
        will handle its own get_html logic.
        This method on the base SyncBlock might not be directly called.
        """
        return None
