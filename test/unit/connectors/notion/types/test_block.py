import pytest

from unstructured_ingest.processes.connectors.notion.types import blocks
from unstructured_ingest.processes.connectors.notion.types.block import Block


def block_payload(block_type: str, block_data: dict) -> dict:
    user = {"object": "user", "id": "user-id"}
    return {
        "object": "block",
        "id": "block-id",
        "type": block_type,
        "created_time": "2024-01-01T00:00:00.000Z",
        "created_by": dict(user),
        "last_edited_time": "2024-01-01T00:00:00.000Z",
        "last_edited_by": dict(user),
        "archived": False,
        "in_trash": False,
        "has_children": False,
        "parent": {"type": "page_id", "page_id": "page-id"},
        block_type: block_data,
    }


@pytest.mark.parametrize("source", ["external", "file"])
def test_audio_block(source: str):
    url = "https://example.com/sound.mp3"
    file_data = {"url": url}
    if source == "file":
        file_data["expiry_time"] = "2024-01-01T01:00:00.000Z"

    block = Block.from_dict(
        block_payload("audio", {"caption": [], "type": source, source: file_data})
    )

    assert isinstance(block.block, blocks.Audio)
    html = block.get_html().render()
    assert "<audio" in html
    assert url in html


def test_unmapped_block_type_is_unsupported():
    block = Block.from_dict(block_payload("some_new_type", {"foo": "bar"}))

    assert block.type == "some_new_type"
    assert isinstance(block.block, blocks.Unsupported)
    assert block.get_html() is None
