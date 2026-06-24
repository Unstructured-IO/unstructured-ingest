"""Unit tests for Notion rich-text parsing and its hardening.

``rich_text`` is parsed inside nearly every block, so a single unmodeled field
on an annotation or text payload would otherwise break the majority of page
content. These tests pin both the rendering behavior and the unknown-key
tolerance across the rich-text leaf types.
"""

from htmlBuilder.tags import Div

from unstructured_ingest.processes.connectors.notion.types.rich_text import (
    Annotations,
    Equation,
    MentionDatabase,
    MentionLinkPreview,
    MentionPage,
    MentionTemplate,
    RichText,
    Text,
)

_FUTURE_KEYS = {"some_future_field": "x", "another_addition": {"nested": True}}


def annotations_payload(**overrides) -> dict:
    base = {
        "bold": False,
        "code": False,
        "italic": False,
        "strikethrough": False,
        "underline": False,
        "color": "default",
    }
    base.update(overrides)
    return base


def render(tag) -> str:
    # RichText.get_html may return a bare ``Text`` leaf (when there is no link
    # or active annotation), which is only renderable as a child. Wrap it so we
    # can assert on the rendered output uniformly.
    return Div([], [tag]).render() if tag is not None else ""


def test_annotations_basic():
    ann = Annotations.from_dict(annotations_payload(bold=True, color="red"))
    assert ann.bold is True
    assert ann.color == "red"


def test_annotations_ignores_unknown_keys():
    ann = Annotations.from_dict(annotations_payload(**_FUTURE_KEYS))
    assert isinstance(ann, Annotations)
    for unexpected in _FUTURE_KEYS:
        assert not hasattr(ann, unexpected)


def test_text_ignores_unknown_keys():
    text = Text.from_dict({"content": "hi", "link": None, **_FUTURE_KEYS})
    assert text.content == "hi"
    assert not hasattr(text, "some_future_field")


def test_rich_text_text_renders_with_annotations():
    rt = RichText.from_dict(
        {
            "type": "text",
            "plain_text": "hello",
            "href": None,
            "annotations": annotations_payload(bold=True),
            "text": {"content": "hello", "link": None},
        }
    )
    out = render(rt.get_html())
    assert "hello" in out
    assert "<b>" in out.lower()


def test_rich_text_ignores_unknown_top_level_keys():
    rt = RichText.from_dict(
        {
            "type": "text",
            "plain_text": "hello",
            "href": None,
            "annotations": annotations_payload(),
            "text": {"content": "hello", "link": None},
            **_FUTURE_KEYS,
        }
    )
    assert isinstance(rt, RichText)
    for unexpected in _FUTURE_KEYS:
        assert not hasattr(rt, unexpected)


def test_rich_text_tolerates_missing_annotations():
    # Defensive: even if Notion ever omits ``annotations``, mapping must hold.
    rt = RichText.from_dict(
        {
            "type": "text",
            "plain_text": "hello",
            "text": {"content": "hello", "link": None},
        }
    )
    assert rt.annotations is None
    assert "hello" in render(rt.get_html())


def test_rich_text_unknown_annotation_field_does_not_break():
    rt = RichText.from_dict(
        {
            "type": "text",
            "plain_text": "hello",
            "href": None,
            "annotations": annotations_payload(highlight="neon"),
            "text": {"content": "hello", "link": None},
        }
    )
    assert "hello" in render(rt.get_html())


def test_mention_leaf_types_ignore_unknown_keys():
    assert MentionDatabase.from_dict({"id": "db", **_FUTURE_KEYS}).id == "db"
    assert MentionPage.from_dict({"id": "pg", **_FUTURE_KEYS}).id == "pg"
    assert (
        MentionLinkPreview.from_dict({"url": "https://x", **_FUTURE_KEYS}).url
        == "https://x"
    )
    tmpl = MentionTemplate.from_dict(
        {"template_mention_date": None, "template_mention_user": None, **_FUTURE_KEYS}
    )
    assert isinstance(tmpl, MentionTemplate)


def test_equation_leaf_ignores_unknown_keys():
    eq = Equation.from_dict({"expression": "x^2", **_FUTURE_KEYS})
    assert "x^2" in render(eq.get_html())
