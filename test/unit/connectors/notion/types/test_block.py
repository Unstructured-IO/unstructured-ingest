"""Unit tests for the ``Block`` dispatcher (``block_type_mapping``)."""

import pytest

from unstructured_ingest.processes.connectors.notion.types.block import (
    Block,
    block_type_mapping,
)
from unstructured_ingest.processes.connectors.notion.types.blocks import (
    Heading,
    NumberedListItem,
    Paragraph,
)


def _rich_text(text: str) -> dict:
    return {
        "type": "text",
        "plain_text": text,
        "href": None,
        "annotations": {
            "bold": False,
            "code": False,
            "italic": False,
            "strikethrough": False,
            "underline": False,
            "color": "default",
        },
        "text": {"content": text, "link": None},
    }


def block_payload(block_type: str, block_data: dict) -> dict:
    """Build a full Notion block object wrapping ``block_data`` under ``block_type``."""
    return {
        "id": "blk-1",
        "type": block_type,
        "created_time": "2026-01-01T00:00:00.000Z",
        "created_by": {"object": "user", "id": "u1"},
        "last_edited_time": "2026-01-01T00:00:00.000Z",
        "last_edited_by": {"object": "user", "id": "u1"},
        "archived": False,
        "in_trash": False,
        "has_children": False,
        "parent": {"type": "page_id", "page_id": "p1"},
        "object": "block",
        block_type: block_data,
    }


def test_block_maps_paragraph():
    block = Block.from_dict(
        block_payload("paragraph", {"color": "default", "rich_text": [_rich_text("hi")]})
    )
    assert block.type == "paragraph"
    assert isinstance(block.block, Paragraph)
    assert "hi" in block.get_html().render()


@pytest.mark.parametrize("heading_type", ["heading_1", "heading_2", "heading_3"])
def test_block_maps_all_headings(heading_type: str):
    block = Block.from_dict(
        block_payload(
            heading_type,
            {"color": "default", "is_toggleable": False, "rich_text": [_rich_text("H")]},
        )
    )
    assert isinstance(block.block, Heading)


def test_block_maps_numbered_list_item_with_list_format():
    # End-to-end regression: ordered-list metadata must not break dispatch.
    block = Block.from_dict(
        block_payload(
            "numbered_list_item",
            {
                "color": "default",
                "list_format": "letters",
                "list_start_index": 2,
                "rich_text": [_rich_text("item")],
            },
        )
    )
    assert isinstance(block.block, NumberedListItem)
    assert block.block.list_format == "letters"
    assert "item" in block.get_html().render()


def test_block_unknown_type_raises_key_error():
    with pytest.raises(KeyError, match="failed to map to associated block type"):
        Block.from_dict(block_payload("non_existent_type", {}))


def test_every_mapped_block_type_is_importable():
    # Guard against a mapping entry pointing at something non-constructable.
    for block_type, cls in block_type_mapping.items():
        assert hasattr(cls, "from_dict"), block_type
        assert hasattr(cls, "get_html"), block_type
