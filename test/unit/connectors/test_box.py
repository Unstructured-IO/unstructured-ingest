from collections import OrderedDict
from unittest.mock import MagicMock

from unstructured_ingest.processes.connectors.fsspec.box import (
    BOX_ROLE_MAPPING,
    _get_permissions_for_file,
    _normalize_collaborations,
)


def make_collab(
    entity_type: str,
    entity_id: str,
    role: str,
    status: str = "accepted",
    group_type: str = "managed_group",
) -> dict:
    accessible_by = {"type": entity_type, "id": entity_id}
    if entity_type == "group":
        accessible_by["group_type"] = group_type
    return {"accessible_by": accessible_by, "role": role, "status": status}


# ---------------------------------------------------------------------------
# BOX_ROLE_MAPPING
# ---------------------------------------------------------------------------


class TestBoxRoleMapping:
    def test_all_expected_roles_present(self):
        expected = {"owner", "co-owner", "editor", "viewer uploader", "viewer"}
        assert set(BOX_ROLE_MAPPING.keys()) == expected

    def test_uploader_excluded(self):
        assert "uploader" not in BOX_ROLE_MAPPING

    def test_previewer_roles_excluded(self):
        # Previewers can render but not download, so they don't get "read".
        assert "previewer" not in BOX_ROLE_MAPPING
        assert "previewer uploader" not in BOX_ROLE_MAPPING

    def test_owner_gets_all_operations(self):
        assert set(BOX_ROLE_MAPPING["owner"]) == {"read", "update", "delete"}

    def test_co_owner_gets_all_operations(self):
        assert set(BOX_ROLE_MAPPING["co-owner"]) == {"read", "update", "delete"}

    def test_editor_gets_all_operations(self):
        assert set(BOX_ROLE_MAPPING["editor"]) == {"read", "update", "delete"}

    def test_read_only_roles(self):
        for role in ("viewer", "viewer uploader"):
            assert BOX_ROLE_MAPPING[role] == ["read"], f"{role} should map to read only"


# ---------------------------------------------------------------------------
# _normalize_collaborations
# ---------------------------------------------------------------------------


class TestNormalizeCollaborations:
    def _empty_normalized(self):
        return {
            "read": {"users": set(), "groups": set()},
            "update": {"users": set(), "groups": set()},
            "delete": {"users": set(), "groups": set()},
        }

    def test_user_owner_gets_all_operations(self):
        normalized = self._empty_normalized()
        total = [0]
        _normalize_collaborations(
            [make_collab("user", "u1", "owner")], normalized, total, max_perms=500
        )
        assert "u1" in normalized["read"]["users"]
        assert "u1" in normalized["update"]["users"]
        assert "u1" in normalized["delete"]["users"]
        assert total[0] == 1

    def test_group_viewer_gets_read_only(self):
        normalized = self._empty_normalized()
        total = [0]
        _normalize_collaborations(
            [make_collab("group", "g1", "viewer")], normalized, total, max_perms=500
        )
        assert "g1" in normalized["read"]["groups"]
        assert "g1" not in normalized["update"]["groups"]
        assert "g1" not in normalized["delete"]["groups"]

    def test_pending_collab_skipped(self):
        normalized = self._empty_normalized()
        total = [0]
        _normalize_collaborations(
            [make_collab("user", "u1", "editor", status="pending")],
            normalized,
            total,
            max_perms=500,
        )
        assert total[0] == 0
        assert not normalized["read"]["users"]

    def test_all_users_group_skipped(self):
        normalized = self._empty_normalized()
        total = [0]
        _normalize_collaborations(
            [make_collab("group", "g1", "editor", group_type="all_users_group")],
            normalized,
            total,
            max_perms=500,
        )
        assert total[0] == 0
        assert not normalized["read"]["groups"]

    def test_access_only_collab_skipped(self):
        normalized = self._empty_normalized()
        total = [0]
        collab = make_collab("user", "u1", "editor")
        collab["is_access_only"] = True
        _normalize_collaborations([collab], normalized, total, max_perms=500)
        assert total[0] == 0
        assert not normalized["read"]["users"]
        assert not normalized["update"]["users"]

    def test_unknown_role_produces_no_operations(self):
        normalized = self._empty_normalized()
        total = [0]
        _normalize_collaborations(
            [make_collab("user", "u1", "some_future_role")], normalized, total, max_perms=500
        )
        assert total[0] == 0

    def test_uploader_role_produces_no_operations(self):
        normalized = self._empty_normalized()
        total = [0]
        _normalize_collaborations(
            [make_collab("user", "u1", "uploader")], normalized, total, max_perms=500
        )
        assert total[0] == 0
        assert not normalized["read"]["users"]

    def test_max_permissions_cap_respected(self):
        normalized = self._empty_normalized()
        total = [0]
        collabs = [make_collab("user", f"u{i}", "viewer") for i in range(5)]
        _normalize_collaborations(collabs, normalized, total, max_perms=2)
        assert total[0] == 2
        assert len(normalized["read"]["users"]) == 2

    def test_missing_accessible_by_skipped(self):
        normalized = self._empty_normalized()
        total = [0]
        _normalize_collaborations(
            [{"accessible_by": None, "role": "editor", "status": "accepted"}],
            normalized,
            total,
            max_perms=500,
        )
        assert total[0] == 0


# ---------------------------------------------------------------------------
# _get_permissions_for_file
# ---------------------------------------------------------------------------


def _make_collab_obj(entity_type, entity_id, role, status="accepted", group_type="managed_group"):
    raw = make_collab(entity_type, entity_id, role, status, group_type)
    obj = MagicMock()
    obj.response_object = raw
    return obj


class TestGetPermissionsForFile:
    def _make_client(self, path_entries, folder_collabs_by_id, file_collabs):
        client = MagicMock()

        file_obj = MagicMock()
        file_obj.response_object = {
            "path_collection": {"entries": path_entries},
            "has_collaborations": True,
        }
        client.file.return_value.get.return_value = file_obj
        client.file.return_value.get_collaborations.return_value = file_collabs

        def folder_side_effect(folder_id):
            folder_mock = MagicMock()
            folder_mock.get_collaborations.return_value = folder_collabs_by_id.get(folder_id, [])
            return folder_mock

        client.folder.side_effect = folder_side_effect
        return client

    def test_basic_file_collaboration(self):
        cache = OrderedDict()
        client = self._make_client(
            path_entries=[],
            folder_collabs_by_id={},
            file_collabs=[_make_collab_obj("user", "u1", "editor")],
        )
        result = _get_permissions_for_file(client, "file123", cache)
        read_entry = next(d for d in result if "read" in d)
        update_entry = next(d for d in result if "update" in d)
        assert "u1" in read_entry["read"]["users"]
        assert "u1" in update_entry["update"]["users"]

    def test_inherited_folder_collaboration(self):
        cache = OrderedDict()
        folder_collab = MagicMock()
        folder_collab.response_object = make_collab("user", "u2", "viewer")
        client = self._make_client(
            path_entries=[{"id": "folder99"}],
            folder_collabs_by_id={"folder99": [folder_collab]},
            file_collabs=[],
        )
        result = _get_permissions_for_file(client, "file123", cache)
        read_entry = next(d for d in result if "read" in d)
        assert "u2" in read_entry["read"]["users"]

    def test_root_folder_skipped(self):
        cache = OrderedDict()
        client = self._make_client(
            path_entries=[{"id": "0"}],
            folder_collabs_by_id={"0": [MagicMock()]},
            file_collabs=[],
        )
        _get_permissions_for_file(client, "file123", cache)
        client.folder.assert_not_called()

    def test_output_ids_are_sorted(self):
        cache = OrderedDict()
        client = self._make_client(
            path_entries=[],
            folder_collabs_by_id={},
            file_collabs=[
                _make_collab_obj("user", "z_user", "viewer"),
                _make_collab_obj("user", "a_user", "viewer"),
            ],
        )
        result = _get_permissions_for_file(client, "file123", cache)
        read_entry = next(d for d in result if "read" in d)
        assert read_entry["read"]["users"] == sorted(read_entry["read"]["users"])

    def test_output_has_all_three_operations(self):
        cache = OrderedDict()
        client = self._make_client(path_entries=[], folder_collabs_by_id={}, file_collabs=[])
        result = _get_permissions_for_file(client, "file123", cache)
        keys = [list(d.keys())[0] for d in result]
        assert set(keys) == {"read", "update", "delete"}

    def test_folder_collab_cache_used(self):
        cache = OrderedDict()
        folder_collab = MagicMock()
        folder_collab.response_object = make_collab("user", "u1", "viewer")
        client = self._make_client(
            path_entries=[{"id": "f1"}],
            folder_collabs_by_id={"f1": [folder_collab]},
            file_collabs=[],
        )
        _get_permissions_for_file(client, "file1", cache)
        _get_permissions_for_file(client, "file2", cache)
        # folder f1 should only be fetched once despite two file calls
        assert client.folder.call_count == 1

    def test_folder_collab_transient_failure_not_cached(self):
        cache = OrderedDict()
        folder_collab = MagicMock()
        folder_collab.response_object = make_collab("user", "u1", "viewer")

        folder_mock = MagicMock()
        folder_mock.get_collaborations.side_effect = [
            Exception("transient 503"),
            [folder_collab],
        ]

        client = MagicMock()
        file_obj = MagicMock()
        file_obj.response_object = {"path_collection": {"entries": [{"id": "f1"}]}}
        client.file.return_value.get.return_value = file_obj
        client.file.return_value.get_collaborations.return_value = []
        client.folder.return_value = folder_mock

        first = _get_permissions_for_file(client, "file1", cache)
        first_read = next(d for d in first if "read" in d)
        assert "u1" not in first_read["read"]["users"]

        # Second call must retry, not serve a cached empty list from the transient failure.
        second = _get_permissions_for_file(client, "file2", cache)
        second_read = next(d for d in second if "read" in d)
        assert "u1" in second_read["read"]["users"]
        assert folder_mock.get_collaborations.call_count == 2

    def test_api_error_on_path_collection_returns_empty(self):
        cache = OrderedDict()
        client = MagicMock()
        client.file.return_value.get.side_effect = Exception("API error")
        client.file.return_value.get_collaborations.return_value = []
        result = _get_permissions_for_file(client, "file123", cache)
        for entry in result:
            for val in list(entry.values())[0].values():
                assert val == []
