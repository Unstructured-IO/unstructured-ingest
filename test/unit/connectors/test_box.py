from collections import OrderedDict
from unittest.mock import MagicMock, patch

import pytest

from unstructured_ingest.processes.connectors.fsspec.box import (
    BOX_ROLE_MAPPING,
    BoxDownloader,
    BoxDownloaderConfig,
)


def make_downloader(max_permissions: int = 500) -> BoxDownloader:
    connection_config = MagicMock()
    download_config = BoxDownloaderConfig(max_num_metadata_permissions=max_permissions)
    downloader = BoxDownloader.__new__(BoxDownloader)
    downloader.connection_config = connection_config
    downloader.download_config = download_config
    downloader._folder_collab_cache = OrderedDict()
    downloader._folder_collab_cache_max_size = 5
    return downloader


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
        expected = {"owner", "co-owner", "editor", "viewer uploader", "previewer uploader",
                    "viewer", "previewer"}
        assert set(BOX_ROLE_MAPPING.keys()) == expected

    def test_uploader_excluded(self):
        assert "uploader" not in BOX_ROLE_MAPPING

    def test_owner_gets_all_operations(self):
        assert set(BOX_ROLE_MAPPING["owner"]) == {"read", "update", "delete"}

    def test_co_owner_gets_all_operations(self):
        assert set(BOX_ROLE_MAPPING["co-owner"]) == {"read", "update", "delete"}

    def test_editor_gets_read_and_update(self):
        assert set(BOX_ROLE_MAPPING["editor"]) == {"read", "update"}

    def test_read_only_roles(self):
        for role in ("viewer", "previewer", "viewer uploader", "previewer uploader"):
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
        downloader = make_downloader()
        normalized = self._empty_normalized()
        total = [0]
        downloader._normalize_collaborations(
            [make_collab("user", "u1", "owner")], normalized, total
        )
        assert "u1" in normalized["read"]["users"]
        assert "u1" in normalized["update"]["users"]
        assert "u1" in normalized["delete"]["users"]
        assert total[0] == 1

    def test_group_viewer_gets_read_only(self):
        downloader = make_downloader()
        normalized = self._empty_normalized()
        total = [0]
        downloader._normalize_collaborations(
            [make_collab("group", "g1", "viewer")], normalized, total
        )
        assert "g1" in normalized["read"]["groups"]
        assert "g1" not in normalized["update"]["groups"]
        assert "g1" not in normalized["delete"]["groups"]

    def test_pending_collab_skipped(self):
        downloader = make_downloader()
        normalized = self._empty_normalized()
        total = [0]
        downloader._normalize_collaborations(
            [make_collab("user", "u1", "editor", status="pending")], normalized, total
        )
        assert total[0] == 0
        assert not normalized["read"]["users"]

    def test_all_users_group_skipped(self):
        downloader = make_downloader()
        normalized = self._empty_normalized()
        total = [0]
        downloader._normalize_collaborations(
            [make_collab("group", "g1", "editor", group_type="all_users_group")],
            normalized,
            total,
        )
        assert total[0] == 0
        assert not normalized["read"]["groups"]

    def test_unknown_role_produces_no_operations(self):
        downloader = make_downloader()
        normalized = self._empty_normalized()
        total = [0]
        downloader._normalize_collaborations(
            [make_collab("user", "u1", "some_future_role")], normalized, total
        )
        assert total[0] == 0

    def test_uploader_role_produces_no_operations(self):
        downloader = make_downloader()
        normalized = self._empty_normalized()
        total = [0]
        downloader._normalize_collaborations(
            [make_collab("user", "u1", "uploader")], normalized, total
        )
        assert total[0] == 0
        assert not normalized["read"]["users"]

    def test_max_permissions_cap_respected(self):
        downloader = make_downloader(max_permissions=2)
        normalized = self._empty_normalized()
        total = [0]
        collabs = [make_collab("user", f"u{i}", "viewer") for i in range(5)]
        downloader._normalize_collaborations(collabs, normalized, total)
        assert total[0] == 2
        assert len(normalized["read"]["users"]) == 2

    def test_missing_accessible_by_skipped(self):
        downloader = make_downloader()
        normalized = self._empty_normalized()
        total = [0]
        downloader._normalize_collaborations(
            [{"accessible_by": None, "role": "editor", "status": "accepted"}],
            normalized,
            total,
        )
        assert total[0] == 0


# ---------------------------------------------------------------------------
# get_permissions_for_file
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
        downloader = make_downloader()
        client = self._make_client(
            path_entries=[],
            folder_collabs_by_id={},
            file_collabs=[_make_collab_obj("user", "u1", "editor")],
        )
        result = downloader.get_permissions_for_file(client, "file123")
        read_entry = next(d for d in result if "read" in d)
        update_entry = next(d for d in result if "update" in d)
        assert "u1" in read_entry["read"]["users"]
        assert "u1" in update_entry["update"]["users"]

    def test_inherited_folder_collaboration(self):
        downloader = make_downloader()
        folder_collab = MagicMock()
        folder_collab.response_object = make_collab("user", "u2", "viewer")
        client = self._make_client(
            path_entries=[{"id": "folder99"}],
            folder_collabs_by_id={"folder99": [folder_collab]},
            file_collabs=[],
        )
        result = downloader.get_permissions_for_file(client, "file123")
        read_entry = next(d for d in result if "read" in d)
        assert "u2" in read_entry["read"]["users"]

    def test_root_folder_skipped(self):
        downloader = make_downloader()
        client = self._make_client(
            path_entries=[{"id": "0"}],
            folder_collabs_by_id={"0": [MagicMock()]},
            file_collabs=[],
        )
        # folder "0" should not be fetched
        result = downloader.get_permissions_for_file(client, "file123")
        client.folder.assert_not_called()

    def test_output_ids_are_sorted(self):
        downloader = make_downloader()
        client = self._make_client(
            path_entries=[],
            folder_collabs_by_id={},
            file_collabs=[
                _make_collab_obj("user", "z_user", "viewer"),
                _make_collab_obj("user", "a_user", "viewer"),
            ],
        )
        result = downloader.get_permissions_for_file(client, "file123")
        read_entry = next(d for d in result if "read" in d)
        assert read_entry["read"]["users"] == sorted(read_entry["read"]["users"])

    def test_output_has_all_three_operations(self):
        downloader = make_downloader()
        client = self._make_client(path_entries=[], folder_collabs_by_id={}, file_collabs=[])
        result = downloader.get_permissions_for_file(client, "file123")
        keys = [list(d.keys())[0] for d in result]
        assert set(keys) == {"read", "update", "delete"}

    def test_folder_collab_cache_used(self):
        downloader = make_downloader()
        folder_collab = MagicMock()
        folder_collab.response_object = make_collab("user", "u1", "viewer")
        client = self._make_client(
            path_entries=[{"id": "f1"}],
            folder_collabs_by_id={"f1": [folder_collab]},
            file_collabs=[],
        )
        downloader.get_permissions_for_file(client, "file1")
        downloader.get_permissions_for_file(client, "file2")
        # folder f1 should only be fetched once despite two file calls
        assert client.folder.call_count == 1

    def test_api_error_on_path_collection_returns_empty(self):
        downloader = make_downloader()
        client = MagicMock()
        client.file.return_value.get.side_effect = Exception("API error")
        client.file.return_value.get_collaborations.return_value = []
        result = downloader.get_permissions_for_file(client, "file123")
        # should not raise; all lists should be empty
        for entry in result:
            for val in list(entry.values())[0].values():
                assert val == []
