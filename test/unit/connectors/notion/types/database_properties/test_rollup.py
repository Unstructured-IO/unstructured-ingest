"""Regression tests for ``rollup`` database-property cell rendering.

An ``array`` rollup embeds raw property-value objects (e.g. a rolled-up title
yields ``{"type": "title", "title": [<rich text>...]}``). The cell previously
rendered each element with ``str()``, leaking the raw Python dict into the HTML
output instead of the underlying text.
"""

from unstructured_ingest.processes.connectors.notion.types.database_properties.rollup import (
    Rollup,
    RollupCell,
    RollupProp,
)


def _cell(rollup: dict) -> RollupCell:
    return RollupCell.from_dict({"id": "c", "type": "rollup", "rollup": rollup})


def _rt(text: str) -> dict:
    return {"type": "text", "plain_text": text}


def test_array_rollup_of_titles_renders_text_not_raw_dict():
    cell = _cell(
        {
            "type": "array",
            "function": "show_original",
            "array": [{"type": "title", "title": [_rt("Row 2 - relation target")]}],
        }
    )
    out = cell.get_html().render()
    assert "Row 2 - relation target" in out
    # The raw API object must not leak into the output.
    assert "'type'" not in out
    assert "plain_text" not in out


def test_empty_array_rollup_returns_none():
    assert _cell({"type": "array", "array": []}).get_html() is None


def test_number_rollup_renders_value():
    assert "42" in _cell({"type": "number", "number": 42}).get_html().render()


def test_zero_number_rollup_still_renders():
    # 0 is falsy but is a legitimate value, not "missing".
    assert "0" in _cell({"type": "number", "number": 0}).get_html().render()


def test_date_rollup_renders_range():
    out = (
        _cell({"type": "date", "date": {"start": "2026-01-01", "end": "2026-02-01"}})
        .get_html()
        .render()
    )
    assert "2026-01-01" in out
    assert "2026-02-01" in out


def test_array_of_mixed_values_renders_text():
    cell = _cell(
        {
            "type": "array",
            "array": [
                {"type": "rich_text", "rich_text": [_rt("hello")]},
                {"type": "number", "number": 7},
                {"type": "select", "select": {"name": "Option A"}},
            ],
        }
    )
    out = cell.get_html().render()
    assert "hello" in out
    assert "7" in out
    assert "Option A" in out
    assert "'type'" not in out


def test_unsupported_rollup_does_not_dump_dict():
    cell = _cell({"type": "unsupported", "unsupported": {"weird": "object"}})
    html = cell.get_html()
    rendered = html.render() if html is not None else ""
    assert "weird" not in rendered
    assert "'type'" not in rendered


def test_rollup_cell_ignores_unknown_keys():
    cell = RollupCell.from_dict(
        {"id": "c", "type": "rollup", "rollup": {"type": "number", "number": 1}, "future": 1}
    )
    assert isinstance(cell, RollupCell)
    assert not hasattr(cell, "future")


def test_rollup_property_ignores_unknown_keys():
    prop = Rollup.from_dict(
        {
            "id": "p",
            "name": "Roll",
            "type": "rollup",
            "rollup": {
                "function": "show_original",
                "relation_property_id": "r",
                "relation_property_name": "Related",
                "rollup_property_id": "t",
                "rollup_property_name": "Title",
                "future_prop_key": True,
            },
            "future_top_key": True,
        }
    )
    assert isinstance(prop.rollup, RollupProp)
    assert not hasattr(prop, "future_top_key")
    assert not hasattr(prop.rollup, "future_prop_key")
