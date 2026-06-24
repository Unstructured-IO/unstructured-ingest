"""Regression tests for the ``relation`` database property.

A one-way relation reports ``type == "single_property"`` (with an empty
sub-object). The parser previously only accepted ``dual_property`` and raised
``ValueError`` otherwise, which aborted extraction of the entire database the
moment it contained a normal one-way relation column.
"""

from unstructured_ingest.processes.connectors.notion.types.database_properties.relation import (
    Relation,
    RelationCell,
    RelationProp,
)


def _relation_prop(rel: dict) -> dict:
    return {"id": "prop-id", "name": "Related", "type": "relation", "relation": rel}


def test_single_property_relation_does_not_raise():
    prop = Relation.from_dict(
        _relation_prop({"database_id": "db-1", "type": "single_property", "single_property": {}})
    )
    assert isinstance(prop, Relation)
    assert prop.relation.type == "single_property"
    assert prop.relation.dual_property is None


def test_dual_property_relation_still_parses():
    prop = Relation.from_dict(
        _relation_prop(
            {
                "database_id": "db-1",
                "type": "dual_property",
                "dual_property": {
                    "synced_property_id": "p-id",
                    "synced_property_name": "Synced",
                },
            }
        )
    )
    assert prop.relation.dual_property is not None
    assert prop.relation.dual_property.synced_property_name == "Synced"


def test_unknown_relation_type_does_not_raise():
    # Future-proofing: an unmodeled relation ``type`` must not abort the DB.
    prop = Relation.from_dict(
        _relation_prop(
            {"database_id": "db-1", "type": "some_future_type", "some_future_type": {}}
        )
    )
    assert prop.relation.type == "some_future_type"
    assert prop.relation.dual_property is None


def test_relation_prop_ignores_unknown_keys():
    rp = RelationProp.from_dict(
        {"database_id": "db-1", "type": "single_property", "single_property": {}, "extra": 1}
    )
    assert rp.database_id == "db-1"
    assert not hasattr(rp, "extra")


def test_relation_cell_renders_and_ignores_unknown_keys():
    cell = RelationCell.from_dict(
        {
            "id": "cell-id",
            "has_more": False,
            "relation": [{"id": "linked-1"}],
            "type": "relation",
            "future_field": True,
        }
    )
    assert cell.has_more is False
    assert "cell-id" in cell.get_html().render()
    assert not hasattr(cell, "future_field")
