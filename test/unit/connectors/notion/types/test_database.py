from unstructured_ingest.processes.connectors.notion.types.database import Database


def database_response(**extra) -> dict:
    user = {"object": "user", "id": "user-1"}
    return {
        "object": "database",
        "id": "db-1",
        "created_time": "2026-09-01T00:00:00.000Z",
        "created_by": user,
        "last_edited_time": "2026-09-02T00:00:00.000Z",
        "last_edited_by": user,
        "archived": False,
        "in_trash": False,
        "parent": {"type": "workspace", "workspace": True},
        "url": "https://www.notion.so/db1",
        "is_inline": False,
        "public_url": None,
        "icon": None,
        "cover": None,
        "title": [],
        "description": [],
        "properties": {},
        **extra,
    }


def test_database_from_dict_ignores_unknown_fields():
    database = Database.from_dict(database_response(database_type="database", new_field={"x": 1}))

    assert database.id == "db-1"
    assert not hasattr(database, "database_type")
