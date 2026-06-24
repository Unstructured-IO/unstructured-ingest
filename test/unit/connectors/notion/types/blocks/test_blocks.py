"""Unit tests for every Notion page-block type.

These exercise each block's ``from_dict`` / ``get_html`` / ``can_have_children``
contract. The block layer previously had no unit coverage, which let a
regression slip in where ``NumberedListItem`` could not be constructed once
Notion started returning ordered-list display metadata (``list_format`` /
``list_start_index``) on the block payload.
"""

import pytest
from htmlBuilder.tags import (
    Code as HtmlCode,
)
from htmlBuilder.tags import (
    Div,
    Hr,
    Img,
    Input,
    Li,
    P,
    Tr,
)

from unstructured_ingest.processes.connectors.notion.types.blocks import (
    PDF,
    Bookmark,
    Breadcrumb,
    BulletedListItem,
    Callout,
    ChildDatabase,
    ChildPage,
    Code,
    Column,
    ColumnList,
    Divider,
    DuplicateSyncedBlock,
    Embed,
    Equation,
    File,
    Heading,
    Image,
    LinkPreview,
    LinkToPage,
    NumberedListItem,
    OriginalSyncedBlock,
    Paragraph,
    Quote,
    SyncBlock,
    Table,
    TableOfContents,
    TableRow,
    Template,
    ToDo,
    Toggle,
    Unsupported,
    Video,
)


def rich_text(text: str) -> dict:
    """Build a minimal Notion ``rich_text`` payload of ``type == "text"``."""
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


def render(tag) -> str:
    return tag.render() if tag is not None else ""


# ---------------------------------------------------------------------------
# Regression: NumberedListItem must tolerate ordered-list display metadata.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("list_format", ["numbers", "letters", "roman"])
def test_numbered_list_item_accepts_list_format(list_format: str):
    block = NumberedListItem.from_dict(
        {
            "color": "default",
            "list_format": list_format,
            "rich_text": [rich_text("item one")],
        }
    )
    assert isinstance(block, NumberedListItem)
    assert block.list_format == list_format
    assert isinstance(block.get_html(), Li)
    assert "item one" in render(block.get_html())


def test_numbered_list_item_accepts_list_start_index():
    block = NumberedListItem.from_dict(
        {"color": "default", "list_format": "numbers", "list_start_index": 5}
    )
    assert block.list_start_index == 5


def test_numbered_list_item_drops_unknown_keys():
    # Future-proofing: keys Notion may add later must not break mapping.
    block = NumberedListItem.from_dict(
        {"color": "default", "some_future_field": {"nested": True}}
    )
    assert isinstance(block, NumberedListItem)
    assert not hasattr(block, "some_future_field")


def test_numbered_list_item_minimal():
    block = NumberedListItem.from_dict({"color": "default"})
    assert block.color == "default"
    assert block.list_format is None
    assert block.list_start_index is None
    assert block.can_have_children() is True


# ---------------------------------------------------------------------------
# Text / list blocks
# ---------------------------------------------------------------------------
def test_paragraph():
    block = Paragraph.from_dict({"color": "default", "rich_text": [rich_text("hello")]})
    assert isinstance(block.get_html(), Div)
    assert "hello" in render(block.get_html())
    assert block.can_have_children() is True


def test_paragraph_empty_renders_break():
    block = Paragraph.from_dict({"color": "default"})
    # Empty paragraph renders a <br/>.
    assert "br" in render(block.get_html()).lower()


def test_bulleted_list_item():
    block = BulletedListItem.from_dict(
        {"color": "default", "rich_text": [rich_text("bullet")]}
    )
    assert isinstance(block.get_html(), Li)
    assert "bullet" in render(block.get_html())
    assert block.can_have_children() is True


def test_heading():
    block = Heading.from_dict(
        {"color": "default", "is_toggleable": False, "rich_text": [rich_text("Title")]}
    )
    assert isinstance(block.get_html(), Div)
    assert "Title" in render(block.get_html())
    assert block.can_have_children() is False


def test_heading_empty_returns_none():
    block = Heading.from_dict({"color": "default", "is_toggleable": False})
    assert block.get_html() is None


def test_quote():
    block = Quote.from_dict({"color": "default", "rich_text": [rich_text("quoted")]})
    assert "quoted" in render(block.get_html())


def test_quote_empty_returns_none():
    assert Quote.from_dict({"color": "default"}).get_html() is None


def test_toggle():
    block = Toggle.from_dict({"color": "default", "rich_text": [rich_text("toggle me")]})
    assert "toggle me" in render(block.get_html())
    assert block.can_have_children() is True


def test_callout_with_emoji_icon():
    block = Callout.from_dict(
        {
            "color": "default",
            "icon": {"type": "emoji", "emoji": "X"},
            "rich_text": [rich_text("note")],
        }
    )
    assert isinstance(block.get_html(), Div)
    assert "note" in render(block.get_html())


def test_todo_checked_renders_checkbox():
    block = ToDo.from_dict(
        {"color": "default", "checked": True, "rich_text": [rich_text("done")]}
    )
    html = block.get_html()
    assert any(isinstance(child, Input) for child in html.inner_html)
    assert "done" in render(html)


def test_code():
    block = Code.from_dict({"language": "python", "rich_text": [rich_text("print(1)")]})
    assert block.language == "python"
    html = block.get_html()
    assert isinstance(html.inner_html[0], HtmlCode)
    assert "print(1)" in render(html)
    assert block.can_have_children() is False


def test_equation():
    block = Equation.from_dict({"expression": "E=mc^2"})
    assert "E=mc^2" in render(block.get_html())


def test_template():
    block = Template.from_dict({"rich_text": [rich_text("Add item")]})
    assert "Add item" in render(block.get_html())


# ---------------------------------------------------------------------------
# Link / media blocks
# ---------------------------------------------------------------------------
def test_bookmark():
    block = Bookmark.from_dict(
        {"url": "https://example.com", "caption": [rich_text("cap")]}
    )
    assert block.url == "https://example.com"
    out = render(block.get_html())
    assert "https://example.com" in out
    assert "cap" in out


def test_bookmark_empty_returns_none():
    assert Bookmark.from_dict({"url": ""}).get_html() is None


def test_embed():
    block = Embed.from_dict({"url": "https://embed.example", "caption": []})
    assert "https://embed.example" in render(block.get_html())


def test_link_preview():
    block = LinkPreview.from_dict({"url": "https://preview.example"})
    assert "https://preview.example" in render(block.get_html())


def test_link_to_page_with_page_id():
    block = LinkToPage.from_dict({"type": "page_id", "page_id": "page-123"})
    assert "page-123" in render(block.get_html())


def test_link_to_page_with_database_id():
    block = LinkToPage.from_dict({"type": "database_id", "database_id": "db-456"})
    assert "db-456" in render(block.get_html())


def test_file_external():
    block = File.from_dict(
        {"type": "external", "external": {"url": "https://f.example/x.txt"}, "caption": []}
    )
    assert "https://f.example/x.txt" in render(block.get_html())


def test_file_internal():
    block = File.from_dict(
        {
            "type": "file",
            "file": {"url": "https://notion.file/x.txt", "expiry_time": "2099-01-01"},
            "caption": [],
        }
    )
    assert "https://notion.file/x.txt" in render(block.get_html())


def test_pdf_external():
    block = PDF.from_dict(
        {"type": "external", "external": {"url": "https://f.example/x.pdf"}}
    )
    assert "https://f.example/x.pdf" in render(block.get_html())


def test_image_external():
    block = Image.from_dict({"type": "external", "external": {"url": "https://img/x.png"}})
    html = block.get_html()
    assert isinstance(html, Img)
    assert "https://img/x.png" in render(html)


def test_video_external():
    block = Video.from_dict({"type": "external", "external": {"url": "https://vid/x.mp4"}})
    assert "https://vid/x.mp4" in render(block.get_html())


# ---------------------------------------------------------------------------
# Child / structural blocks
# ---------------------------------------------------------------------------
def test_child_page():
    block = ChildPage.from_dict({"title": "My Subpage"})
    assert isinstance(block.get_html(), P)
    assert "My Subpage" in render(block.get_html())
    assert block.can_have_children() is True


def test_child_database():
    block = ChildDatabase.from_dict({"title": "My DB"})
    assert isinstance(block.get_html(), P)
    assert "My DB" in render(block.get_html())


def test_divider():
    block = Divider.from_dict({})
    assert isinstance(block.get_html(), Hr)
    assert block.can_have_children() is False


def test_breadcrumb():
    block = Breadcrumb.from_dict({})
    assert block.get_html() is None
    assert block.can_have_children() is False


def test_table_of_contents():
    block = TableOfContents.from_dict({"color": "default"})
    assert block.get_html() is None


def test_column_list_and_column():
    assert ColumnList.from_dict({}).get_html() is None
    assert Column.from_dict({}).get_html() is None
    assert ColumnList.from_dict({}).can_have_children() is True


def test_unsupported():
    block = Unsupported.from_dict({})
    assert block.get_html() is None


# ---------------------------------------------------------------------------
# Table blocks
# ---------------------------------------------------------------------------
def test_table():
    block = Table.from_dict(
        {"table_width": 2, "has_column_header": True, "has_row_header": False}
    )
    assert block.table_width == 2
    assert block.has_column_header is True
    assert block.get_html() is None


def test_table_row():
    block = TableRow.from_dict(
        {"cells": [[rich_text("a")], [rich_text("b")]]}
    )
    html = block.get_html()
    assert isinstance(html, Tr)
    out = render(html)
    assert "a" in out
    assert "b" in out


# ---------------------------------------------------------------------------
# Synced blocks
# ---------------------------------------------------------------------------
def test_synced_block_original():
    block = SyncBlock.from_dict({"synced_from": None, "children": [{"type": "paragraph"}]})
    assert isinstance(block, OriginalSyncedBlock)
    assert block.get_html() is None


def test_synced_block_duplicate():
    block = SyncBlock.from_dict(
        {"synced_from": {"type": "block_id", "block_id": "abc-123"}}
    )
    assert isinstance(block, DuplicateSyncedBlock)
    assert block.block_id == "abc-123"


def test_synced_block_empty_defaults_to_original():
    block = SyncBlock.from_dict({})
    assert isinstance(block, OriginalSyncedBlock)
    assert block.children == []


def test_synced_block_duplicate_missing_keys_raises():
    with pytest.raises(ValueError):
        SyncBlock.from_dict({"synced_from": {"type": "block_id"}})


# ---------------------------------------------------------------------------
# Hardening: every block type must tolerate unknown keys Notion may add later.
#
# The NumberedListItem outage happened because Notion started returning an
# additive, backwards-compatible field (``list_format``) that the dataclass
# rejected. The same ``cls(**data)`` shape existed across the block layer, so
# these tests pin the contract: unexpected keys are dropped, not fatal.
# ---------------------------------------------------------------------------
_FUTURE_KEYS = {"some_future_field": "x", "another_addition": {"nested": [1, 2]}}

_HARDENED_BLOCK_CASES = [
    (Paragraph, {"color": "default", "rich_text": [rich_text("p")]}),
    (Heading, {"color": "default", "is_toggleable": False, "rich_text": [rich_text("h")]}),
    (Quote, {"color": "default", "rich_text": [rich_text("q")]}),
    (Toggle, {"color": "default", "rich_text": [rich_text("t")]}),
    (ToDo, {"color": "default", "checked": False, "rich_text": [rich_text("td")]}),
    (Template, {"rich_text": [rich_text("tmpl")]}),
    (TableOfContents, {"color": "default"}),
    (NumberedListItem, {"color": "default", "rich_text": [rich_text("nl")]}),
    (ChildDatabase, {"title": "db"}),
    (ChildPage, {"title": "pg"}),
    (Embed, {"url": "https://e.example", "caption": []}),
    (Equation, {"expression": "x^2"}),
    (LinkPreview, {"url": "https://p.example"}),
    (LinkToPage, {"type": "page_id", "page_id": "abc"}),
    (Table, {"table_width": 1, "has_column_header": False, "has_row_header": False}),
    (Callout, {"color": "default", "rich_text": [rich_text("c")]}),
]


@pytest.mark.parametrize(
    "block_cls,payload",
    _HARDENED_BLOCK_CASES,
    ids=[case[0].__name__ for case in _HARDENED_BLOCK_CASES],
)
def test_block_from_dict_ignores_unknown_keys(block_cls, payload):
    block = block_cls.from_dict({**payload, **_FUTURE_KEYS})
    assert isinstance(block, block_cls)
    for unexpected in _FUTURE_KEYS:
        assert not hasattr(block, unexpected)


def test_link_to_page_unknown_variant_does_not_raise():
    # Notion has variants (e.g. ``comment_id``) the dataclass doesn't model.
    # Mapping must not raise; unmodeled targets simply render nothing.
    block = LinkToPage.from_dict({"type": "comment_id", "comment_id": "xyz"})
    assert block.type == "comment_id"
    assert block.get_html() is None


def test_callout_icon_subtypes_ignore_unknown_keys():
    block = Callout.from_dict(
        {
            "color": "default",
            "icon": {"type": "external", "external": {"url": "https://i", "z": 1}, "z": 2},
            "rich_text": [rich_text("c")],
            "future": True,
        }
    )
    assert isinstance(block, Callout)
    assert "https://i" in render(block.get_html())
