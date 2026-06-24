"""Unit tests for the shared ``init_from_dict`` dataclass constructor.

This helper centralizes the defense that keeps Notion parsing resilient to
additive API changes: keys that aren't declared fields are dropped instead of
raising ``TypeError``. It backs the fix for the ``NumberedListItem`` outage and
is now used across the block / rich_text / file layers.
"""

from dataclasses import dataclass, field
from typing import List, Optional

import pytest

from unstructured_ingest.processes.connectors.notion.interfaces import init_from_dict


@dataclass
class _Sample:
    required: str
    optional: Optional[int] = None
    items: List[int] = field(default_factory=list)


def test_keeps_known_fields():
    obj = init_from_dict(_Sample, {"required": "a", "optional": 3})
    assert obj.required == "a"
    assert obj.optional == 3


def test_drops_unknown_fields():
    obj = init_from_dict(_Sample, {"required": "a", "surprise": "ignored"})
    assert obj.required == "a"
    assert not hasattr(obj, "surprise")


def test_overrides_win_over_data():
    # An already-parsed nested value passed as an override should replace the
    # raw value in ``data`` rather than collide with it.
    obj = init_from_dict(_Sample, {"required": "a", "items": [1, 2]}, items=[9])
    assert obj.items == [9]


def test_override_for_popped_key():
    # Typical call shape: the caller pops a key, parses it, and passes the
    # parsed object back through ``overrides``.
    data = {"required": "a", "optional": 1, "extra": "x"}
    parsed = data.pop("optional")
    obj = init_from_dict(_Sample, data, optional=parsed + 10)
    assert obj.optional == 11
    assert not hasattr(obj, "extra")


def test_missing_required_field_still_raises():
    # Hardening only tolerates *extra* keys; a genuinely missing required field
    # is a real error and must still surface.
    with pytest.raises(TypeError):
        init_from_dict(_Sample, {"optional": 1})


def test_empty_data():
    obj = init_from_dict(_Sample, {}, required="a")
    assert obj.required == "a"
    assert obj.optional is None
    assert obj.items == []
