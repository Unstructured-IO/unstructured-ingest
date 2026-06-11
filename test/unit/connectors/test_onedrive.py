from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests
from pydantic import Secret

from unstructured_ingest.error import UserAuthError, ValueError
from unstructured_ingest.processes.connectors.onedrive import (
    MICROSOFT_ROLE_MAPPING,
    OnedriveAccessConfig,
    OnedriveConnectionConfig,
    OnedriveIndexer,
    OnedriveIndexerConfig,
)


class TestOnedriveAccessConfig:
    """Tests for OnedriveAccessConfig authentication validation."""

    def test_client_cred_only(self):
        """Client credential alone should be valid (app-only authentication)."""
        config = OnedriveAccessConfig(client_cred="secret-value")
        assert config.client_cred == "secret-value"
        assert config.oauth_token is None

    def test_client_cred_and_password(self):
        """client_cred + password is the password-grant flow and should be valid."""
        config = OnedriveAccessConfig(client_cred="secret-value", password="user-password")
        assert config.client_cred == "secret-value"
        assert config.password == "user-password"
        assert config.oauth_token is None

    def test_oauth_token_only(self):
        """OAuth token alone should be valid (delegated authentication)."""
        config = OnedriveAccessConfig(oauth_token="ey.access.token")
        assert config.oauth_token == "ey.access.token"
        assert config.client_cred is None

    def test_no_auth_raises_error(self):
        """No authentication provided should raise ValueError."""
        with pytest.raises(ValueError, match="must be set"):
            OnedriveAccessConfig()

    def test_oauth_and_client_cred_raises_error(self):
        """Both oauth_token and client_cred provided should raise ValueError."""
        with pytest.raises(ValueError, match="cannot use both"):
            OnedriveAccessConfig(
                client_cred="secret-value",
                oauth_token="ey.access.token",
            )

    def test_oauth_and_password_raises_error(self):
        """oauth_token combined with password should raise ValueError."""
        with pytest.raises(ValueError, match="cannot use both"):
            OnedriveAccessConfig(
                password="user-password",
                oauth_token="ey.access.token",
            )

    def test_empty_oauth_token_treated_as_missing(self):
        """An empty-string oauth_token (e.g. unset env var) should not satisfy the auth requirement.

        Validator and runtime both use truthiness; this test pins that consistency.
        """
        with pytest.raises(ValueError, match="must be set"):
            OnedriveAccessConfig(oauth_token="")


class TestOnedriveConnectionConfig:
    """Tests for OnedriveConnectionConfig cross-field auth validation."""

    def test_client_cred_without_client_id_raises(self):
        """client_cred-based auth requires client_id; rejecting at config time
        avoids cryptic AADSTS / MSAL errors at runtime."""
        with pytest.raises(ValueError, match="client_id is required"):
            OnedriveConnectionConfig(
                user_pname="alice@contoso.com",
                tenant="tenant-id",
                access_config=Secret(OnedriveAccessConfig(client_cred="secret-value")),
            )

    def test_oauth_token_without_client_id_succeeds(self):
        """oauth_token auth doesn't need client_id; this is the delegated path."""
        config = OnedriveConnectionConfig(
            user_pname="alice@contoso.com",
            tenant="tenant-id",
            access_config=Secret(OnedriveAccessConfig(oauth_token="ey.access.token")),
        )
        assert config.client_id is None


# ---------------------------------------------------------------------------
# Permission extraction
# ---------------------------------------------------------------------------


class TestExtractIdentityIdsFromRaw:
    def test_azure_ad_user_from_granted_to_v2(self):
        raw = {
            "grantedToV2": {
                "user": {"id": "user-uuid-1", "displayName": "Alice"},
            },
        }
        users, groups = OnedriveIndexer._extract_identity_ids_from_raw(raw)
        assert users == {"user-uuid-1"}
        assert groups == set()

    def test_azure_ad_group_from_granted_to_v2(self):
        raw = {
            "grantedToV2": {
                "group": {"id": "group-uuid-1", "displayName": "Owners M365"},
            },
        }
        users, groups = OnedriveIndexer._extract_identity_ids_from_raw(raw)
        assert users == set()
        assert groups == {"group-uuid-1"}

    def test_falls_back_to_granted_to_when_v2_missing(self):
        raw = {
            "grantedTo": {
                "user": {"id": "user-uuid-fallback"},
            },
        }
        users, groups = OnedriveIndexer._extract_identity_ids_from_raw(raw)
        assert users == {"user-uuid-fallback"}

    def test_prefers_v2_over_granted_to(self):
        raw = {
            "grantedToV2": {"user": {"id": "v2-user"}},
            "grantedTo": {"user": {"id": "v1-user"}},
        }
        users, _ = OnedriveIndexer._extract_identity_ids_from_raw(raw)
        assert users == {"v2-user"}

    def test_link_based_grants(self):
        raw = {
            "grantedToIdentitiesV2": [
                {"user": {"id": "link-user-1"}},
                {"group": {"id": "link-group-1"}},
            ],
        }
        users, groups = OnedriveIndexer._extract_identity_ids_from_raw(raw)
        assert users == {"link-user-1"}
        assert groups == {"link-group-1"}

    def test_skips_entries_without_id(self):
        raw = {
            "grantedToV2": {
                "user": {"displayName": "No ID User"},
                "group": {"displayName": "No ID Group"},
            },
        }
        users, groups = OnedriveIndexer._extract_identity_ids_from_raw(raw)
        assert users == set()
        assert groups == set()

    def test_empty_props_returns_empty(self):
        users, groups = OnedriveIndexer._extract_identity_ids_from_raw({})
        assert users == set()
        assert groups == set()

    def test_v2_sitegroup_does_not_fall_through_to_v1(self):
        """When grantedToV2 exists but contains only a siteGroup, we should not
        fall through to grantedTo because v2 supersedes v1 entirely."""
        raw = {
            "grantedToV2": {
                "siteGroup": {"id": "3", "displayName": "Owners"},
            },
            "grantedTo": {
                "user": {"id": "stale-v1-user"},
            },
        }
        users, groups = OnedriveIndexer._extract_identity_ids_from_raw(raw)
        assert users == set()
        assert groups == set()

    def test_site_group_numeric_ids_ignored(self):
        """siteGroup entries lack Azure AD mapping and sit under a different key."""
        raw = {
            "grantedToV2": {
                "siteGroup": {"id": "3", "displayName": "Owners"},
            },
        }
        users, groups = OnedriveIndexer._extract_identity_ids_from_raw(raw)
        assert users == set()
        assert groups == set()


class TestExtractPermissions:
    @pytest.fixture
    def indexer(self):
        indexer = Mock(spec=OnedriveIndexer)
        indexer.extract_permissions = OnedriveIndexer.extract_permissions.__get__(
            indexer, OnedriveIndexer
        )
        indexer._extract_identity_ids_from_raw = OnedriveIndexer._extract_identity_ids_from_raw
        return indexer

    def test_empty_permissions_returns_empty_dict_list(self, indexer):
        result = indexer.extract_permissions([])
        assert result == [{}]

    def test_owner_role_maps_to_all_operations(self, indexer):
        result = indexer.extract_permissions(
            [
                {
                    "roles": ["owner"],
                    "grantedToV2": {"user": {"id": "owner-uuid"}},
                }
            ]
        )
        assert result == [
            {"read": {"users": ["owner-uuid"], "groups": []}},
            {"update": {"users": ["owner-uuid"], "groups": []}},
            {"delete": {"users": ["owner-uuid"], "groups": []}},
        ]

    def test_read_role_maps_to_read_only(self, indexer):
        result = indexer.extract_permissions(
            [
                {
                    "roles": ["read"],
                    "grantedToV2": {"user": {"id": "reader-uuid"}},
                }
            ]
        )
        assert result == [
            {"read": {"users": ["reader-uuid"], "groups": []}},
            {"update": {"users": [], "groups": []}},
            {"delete": {"users": [], "groups": []}},
        ]

    def test_write_role_maps_to_read_and_update(self, indexer):
        result = indexer.extract_permissions(
            [
                {
                    "roles": ["write"],
                    "grantedToV2": {"group": {"id": "writers-group"}},
                }
            ]
        )
        assert result == [
            {"read": {"users": [], "groups": ["writers-group"]}},
            {"update": {"users": [], "groups": ["writers-group"]}},
            {"delete": {"users": [], "groups": []}},
        ]

    def test_multiple_permissions_merged(self, indexer):
        perms = [
            {
                "roles": ["owner"],
                "grantedToV2": {"group": {"id": "owners-group"}},
            },
            {
                "roles": ["write"],
                "grantedToV2": {"user": {"id": "kevin-uuid"}},
            },
            {
                "roles": ["read"],
                "grantedToV2": {"user": {"id": "visitor-uuid"}},
            },
        ]
        result = indexer.extract_permissions(perms)
        assert result == [
            {"read": {"users": sorted(["kevin-uuid", "visitor-uuid"]), "groups": ["owners-group"]}},
            {"update": {"users": ["kevin-uuid"], "groups": ["owners-group"]}},
            {"delete": {"users": [], "groups": ["owners-group"]}},
        ]

    def test_site_group_only_permission_skipped(self, indexer):
        result = indexer.extract_permissions(
            [
                {
                    "roles": ["read"],
                    "grantedToV2": {
                        "siteGroup": {"id": "4", "displayName": "Visitors"},
                    },
                }
            ]
        )
        assert result == [
            {"read": {"users": [], "groups": []}},
            {"update": {"users": [], "groups": []}},
            {"delete": {"users": [], "groups": []}},
        ]

    def test_unmapped_role_logged_and_skipped(self, indexer):
        result = indexer.extract_permissions(
            [
                {
                    "roles": ["some_future_role"],
                    "grantedToV2": {"user": {"id": "user-1"}},
                }
            ]
        )
        assert result == [
            {"read": {"users": [], "groups": []}},
            {"update": {"users": [], "groups": []}},
            {"delete": {"users": [], "groups": []}},
        ]

    def test_limited_access_role_produces_no_operations(self, indexer):
        result = indexer.extract_permissions(
            [
                {
                    "roles": ["sp.limited access"],
                    "grantedToV2": {"user": {"id": "user-1"}},
                }
            ]
        )
        assert result == [
            {"read": {"users": [], "groups": []}},
            {"update": {"users": [], "groups": []}},
            {"delete": {"users": [], "groups": []}},
        ]

    def test_ids_are_sorted_for_deterministic_output(self, indexer):
        perms = [
            {"roles": ["read"], "grantedToV2": {"user": {"id": "zzz-user"}}},
            {"roles": ["read"], "grantedToV2": {"user": {"id": "aaa-user"}}},
        ]
        result = indexer.extract_permissions(perms)
        read_users = result[0]["read"]["users"]
        assert read_users == ["aaa-user", "zzz-user"]

    def test_output_matches_google_drive_schema(self, indexer):
        result = indexer.extract_permissions(
            [{"roles": ["read"], "grantedToV2": {"user": {"id": "u1"}}}]
        )
        assert len(result) == 3
        assert list(result[0].keys()) == ["read"]
        assert list(result[1].keys()) == ["update"]
        assert list(result[2].keys()) == ["delete"]
        for entry in result:
            val = list(entry.values())[0]
            assert set(val.keys()) == {"users", "groups"}
            assert isinstance(val["users"], list)
            assert isinstance(val["groups"], list)


# ---------------------------------------------------------------------------
# DriveItem -> FileData with permissions kwarg
# ---------------------------------------------------------------------------


def _make_drive_item(name: str = "test.docx") -> Mock:
    drive_item = Mock()
    drive_item.name = name
    drive_item.parent_reference.path = "/drives/d1/root:"
    # office365-rest-python-client exposes this as camelCase `driveId`
    drive_item.parent_reference.driveId = "d1"
    drive_item.last_modified_datetime = None
    drive_item.created_datetime = None
    drive_item.id = f"item-{name}"
    drive_item.etag = "etag-1"
    drive_item.properties = {}
    return drive_item


def _make_indexer() -> OnedriveIndexer:
    conn = Mock(spec=OnedriveConnectionConfig)
    conn.user_pname = "test@example.com"
    idx_config = Mock(spec=OnedriveIndexerConfig)
    idx_config.path = ""
    return OnedriveIndexer(connection_config=conn, index_config=idx_config)


class TestDriveItemToFileDataSync:
    def test_permissions_attached_when_passed(self):
        indexer = _make_indexer()
        drive_item = _make_drive_item()
        raw_perms = [
            {
                "roles": ["read"],
                "grantedToV2": {"user": {"id": "user-1"}},
            }
        ]
        file_data = indexer.drive_item_to_file_data_sync(
            drive_item, raw_permissions=raw_perms
        )
        assert file_data.metadata.permissions_data is not None
        assert len(file_data.metadata.permissions_data) == 3

    def test_permissions_none_when_kwarg_omitted(self):
        indexer = _make_indexer()
        drive_item = _make_drive_item()
        file_data = indexer.drive_item_to_file_data_sync(drive_item)
        assert file_data.metadata.permissions_data is None

    def test_permissions_none_when_empty_list_passed(self):
        """Empty raw_permissions (e.g. from a 403 fall-back) should leave the
        field as None, matching the previous behavior of suppressing the
        all-empty {users:[],groups:[]} placeholder."""
        indexer = _make_indexer()
        drive_item = _make_drive_item()
        file_data = indexer.drive_item_to_file_data_sync(drive_item, raw_permissions=[])
        assert file_data.metadata.permissions_data is None


# ---------------------------------------------------------------------------
# Graph /$batch raw fetch
# ---------------------------------------------------------------------------


def _batch_response(status_code: int = 200, responses: list[dict[str, Any]] | None = None) -> Mock:
    resp = Mock()
    resp.status_code = status_code
    resp.json.return_value = {"responses": responses or []}
    resp.text = ""
    return resp


class TestFetchPermissionsRaw:
    def test_empty_drive_items_short_circuits(self):
        indexer = _make_indexer()
        with patch("requests.post") as mock_post:
            result = indexer._fetch_permissions_raw([], access_token="tok")
        assert result == {}
        mock_post.assert_not_called()

    def test_happy_path_returns_per_item_dicts(self):
        indexer = _make_indexer()
        items = [_make_drive_item(f"f{i}.docx") for i in range(3)]
        body = _batch_response(
            responses=[
                {
                    "id": "0",
                    "status": 200,
                    "body": {"value": [{"roles": ["read"], "grantedToV2": {"user": {"id": "a"}}}]},
                },
                {
                    "id": "1",
                    "status": 200,
                    "body": {"value": [{"roles": ["read"], "grantedToV2": {"user": {"id": "b"}}}]},
                },
                {
                    "id": "2",
                    "status": 200,
                    "body": {"value": [{"roles": ["read"], "grantedToV2": {"user": {"id": "c"}}}]},
                },
            ]
        )
        with patch("requests.post", return_value=body) as mock_post:
            result = indexer._fetch_permissions_raw(items, access_token="tok")

        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "https://graph.microsoft.com/v1.0/$batch"
        assert kwargs["headers"]["Authorization"] == "Bearer tok"
        assert kwargs["json"] == {
            "requests": [
                {"id": "0", "method": "GET", "url": "/drives/d1/items/item-f0.docx/permissions"},
                {"id": "1", "method": "GET", "url": "/drives/d1/items/item-f1.docx/permissions"},
                {"id": "2", "method": "GET", "url": "/drives/d1/items/item-f2.docx/permissions"},
            ]
        }
        # Each item maps to its own list, all distinct (no singleton bleed)
        assert {di_id: r[0]["grantedToV2"]["user"]["id"] for di_id, r in result.items()} == {
            "item-f0.docx": "a",
            "item-f1.docx": "b",
            "item-f2.docx": "c",
        }

    def test_per_item_403_yields_empty_for_that_item(self):
        indexer = _make_indexer()
        items = [_make_drive_item(f"f{i}.docx") for i in range(2)]
        body = _batch_response(
            responses=[
                {
                    "id": "0",
                    "status": 200,
                    "body": {"value": [{"roles": ["read"], "grantedToV2": {"user": {"id": "a"}}}]},
                },
                {"id": "1", "status": 403, "body": {"error": {"message": "forbidden"}}},
            ]
        )
        with patch("requests.post", return_value=body):
            result = indexer._fetch_permissions_raw(items, access_token="tok")

        assert result["item-f0.docx"][0]["grantedToV2"]["user"]["id"] == "a"
        assert result["item-f1.docx"] == []

    def test_envelope_401_raises_user_auth_error(self):
        indexer = _make_indexer()
        items = [_make_drive_item("f.docx")]
        body = _batch_response(status_code=401)
        with patch("requests.post", return_value=body):
            with pytest.raises(UserAuthError, match="Unauthorized"):
                indexer._fetch_permissions_raw(items, access_token="bad-token")

    def test_envelope_500_degrades_gracefully(self):
        indexer = _make_indexer()
        items = [_make_drive_item(f"f{i}.docx") for i in range(2)]
        body = _batch_response(status_code=500)
        body.text = "internal server error"
        with patch("requests.post", return_value=body):
            result = indexer._fetch_permissions_raw(items, access_token="tok")
        assert result == {"item-f0.docx": [], "item-f1.docx": []}

    def test_429_triggers_retry_then_succeeds(self):
        indexer = _make_indexer()
        items = [_make_drive_item("f.docx")]
        throttled = _batch_response(status_code=429)
        success = _batch_response(
            responses=[
                {
                    "id": "0",
                    "status": 200,
                    "body": {"value": [{"roles": ["read"], "grantedToV2": {"user": {"id": "x"}}}]},
                }
            ]
        )
        with patch("requests.post", side_effect=[throttled, success]) as mock_post, \
             patch("tenacity.wait_exponential.__call__", return_value=0):
            result = indexer._fetch_permissions_raw(items, access_token="tok")
        assert mock_post.call_count == 2
        assert result["item-f.docx"][0]["grantedToV2"]["user"]["id"] == "x"

    def test_network_errors_exhaust_retries_then_degrade(self):
        indexer = _make_indexer()
        items = [_make_drive_item(f"f{i}.docx") for i in range(2)]
        with patch(
            "requests.post",
            side_effect=requests.exceptions.ConnectionError("dns down"),
        ) as mock_post, \
             patch("tenacity.wait_exponential.__call__", return_value=0):
            result = indexer._fetch_permissions_raw(items, access_token="tok")
        assert mock_post.call_count == 5  # stop_after_attempt(5)
        assert result == {"item-f0.docx": [], "item-f1.docx": []}

    def test_malformed_sub_response_id_skipped(self):
        indexer = _make_indexer()
        items = [_make_drive_item("f.docx")]
        body = _batch_response(
            responses=[
                {"id": "not-an-int", "status": 200, "body": {"value": []}},
                {"id": "99", "status": 200, "body": {"value": []}},  # out of range
            ]
        )
        with patch("requests.post", return_value=body):
            result = indexer._fetch_permissions_raw(items, access_token="tok")
        # malformed entries are skipped; default empty list remains
        assert result == {"item-f.docx": []}


# ---------------------------------------------------------------------------
# Singleton-bleed regression: this is THE test that would have caught the bug
# ---------------------------------------------------------------------------


class TestSingletonBleedRegression:
    """Regression test for the office365-rest-python-client mutable-default-arg
    bug in IdentitySet/SharePointIdentitySet.

    Demonstrates:
      1. The SDK's typed accessors DO collapse all permissions to the last user
         deserialized (proves the bug exists).
      2. Our raw-JSON path through _extract_identity_ids_from_raw + _parse_batch_response
         is unaffected (proves the fix works).

    If anyone refactors back to typed accessors (perm.granted_to_v2.user.id,
    Permission.to_json(), etc.) this test fails loudly.
    """

    def test_office365_sdk_typed_accessor_is_corrupted(self):
        """Sanity-pin the upstream bug. If this ever passes (returns distinct IDs)
        the upstream has been fixed and we can simplify our workaround."""
        try:
            from office365.graph_client import GraphClient
            from office365.onedrive.driveitems.driveItem import DriveItem
            from office365.runtime.paths.resource_path import ResourcePath
        except ImportError:
            pytest.skip("office365-rest-python-client not installed")

        client = GraphClient(lambda: {"access_token": "x", "token_type": "Bearer"})
        di = DriveItem(client, ResourcePath("items/abc"))
        perms = di.permissions
        di.properties["permissions"] = perms
        perms.clear_state()
        for i, uid in enumerate(["alice", "bob", "carol"]):
            perms.set_property(
                i,
                {"id": f"p{i}", "roles": ["read"], "grantedToV2": {"user": {"id": uid}}},
            )

        # All three Permission objects share the same Identity singleton, so
        # every typed read returns the LAST user written.
        seen = {p.granted_to_v2.user.id for p in di.permissions}
        # If office365 ever fixes the bug, this set will become {"alice", "bob", "carol"}
        # and we should remove the workaround. Until then, expect collapse.
        assert len(seen) == 1, (
            "Upstream office365 IdentitySet may have been fixed; "
            "consider removing the raw-JSON workaround in onedrive.py"
        )

    def test_raw_json_path_preserves_distinct_users(self):
        """The fix: parsing raw $batch response dicts directly never touches
        the office365 singletons, so all three users round-trip cleanly."""
        indexer = _make_indexer()
        items = [_make_drive_item(f"f{i}.docx") for i in range(3)]

        # Simulate Graph $batch returning three items with three different users
        batch_payload = {
            "responses": [
                {
                    "id": str(i),
                    "status": 200,
                    "body": {
                        "value": [
                            {"roles": ["read"], "grantedToV2": {"user": {"id": uid}}}
                        ]
                    },
                }
                for i, uid in enumerate(["alice", "bob", "carol"])
            ]
        }

        by_id = indexer._parse_batch_response(batch_payload, items)
        # Each item maps to its own raw permission dict — no bleed.
        seen_ids = [
            by_id[di.id][0]["grantedToV2"]["user"]["id"] for di in items
        ]
        assert seen_ids == ["alice", "bob", "carol"]

        # And running them through extract_permissions yields three distinct users
        # in the read bucket.
        merged_raw_perms = [p for di in items for p in by_id[di.id]]
        result = indexer.extract_permissions(merged_raw_perms)
        assert sorted(result[0]["read"]["users"]) == ["alice", "bob", "carol"]


# ---------------------------------------------------------------------------
# Role mapping
# ---------------------------------------------------------------------------


class TestRoleMapping:
    def test_all_roles_map_to_valid_operations(self):
        valid_ops = {"read", "update", "delete"}
        for role, ops in MICROSOFT_ROLE_MAPPING.items():
            for op in ops:
                assert op in valid_ops, f"role {role!r} maps to invalid op {op!r}"

    def test_read_is_subset_of_write(self):
        read_ops = set(MICROSOFT_ROLE_MAPPING["read"])
        write_ops = set(MICROSOFT_ROLE_MAPPING["write"])
        assert read_ops.issubset(write_ops)

    def test_write_is_subset_of_owner(self):
        write_ops = set(MICROSOFT_ROLE_MAPPING["write"])
        owner_ops = set(MICROSOFT_ROLE_MAPPING["owner"])
        assert write_ops.issubset(owner_ops)
