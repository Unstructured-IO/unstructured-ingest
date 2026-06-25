"""Regression tests for ``unique_id`` cell rendering.

When a unique-ID column has no configured prefix, Notion returns
``prefix: null``. The cell previously rendered ``f"{prefix}-{number}"`` which
produced ``"None-2"``; with no prefix it must show just the number.
"""

from unstructured_ingest.processes.connectors.notion.types.database_properties.unique_id import (
    UniqueID,
    UniqueIDCell,
)


def _cell(unique_id: dict) -> UniqueIDCell:
    return UniqueIDCell.from_dict({"id": "c", "type": "unique_id", "unique_id": unique_id})


def test_no_prefix_renders_number_only():
    out = _cell({"prefix": None, "number": 2}).get_html().render()
    assert "2" in out
    assert "None-2" not in out
    assert "None" not in out


def test_with_prefix_renders_prefixed():
    out = _cell({"prefix": "TASK", "number": 7}).get_html().render()
    assert "TASK-7" in out


def test_empty_string_prefix_renders_number_only():
    out = _cell({"prefix": "", "number": 5}).get_html().render()
    assert "5" in out
    assert "-5" not in out


def test_cell_ignores_unknown_keys():
    # Unknown keys at both the cell level and inside the nested unique_id object.
    cell = UniqueIDCell.from_dict(
        {
            "id": "c",
            "type": "unique_id",
            "unique_id": {"prefix": "T", "number": 3, "nested_future": 1},
            "future": 1,
        }
    )
    assert cell.unique_id.number == 3
    assert not hasattr(cell, "future")
    assert not hasattr(cell.unique_id, "nested_future")


def test_property_ignores_unknown_keys():
    prop = UniqueID.from_dict(
        {"id": "p", "name": "ID", "type": "unique_id", "unique_id": {}, "future": 1}
    )
    assert isinstance(prop, UniqueID)
    assert not hasattr(prop, "future")
