from unittest.mock import Mock

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.processes.connectors.sharepoint import (
    MICROSOFT_ROLE_MAPPING,
    SharepointConnectionConfig,
    SharepointDownloader,
    SharepointDownloaderConfig,
    SharepointIndexer,
    SharepointIndexerConfig,
)


@pytest.fixture
def mock_client():
    return Mock()


@pytest.fixture
def mock_site():
    return Mock()


@pytest.fixture
def mock_drive_item():
    return Mock()


@pytest.fixture
def mock_file():
    return Mock()


@pytest.fixture
def mock_connection_config(mock_client, mock_drive_item):
    config = Mock(spec=SharepointConnectionConfig)
    config.site = "https://test.sharepoint.com/sites/test"
    config.get_client.return_value = mock_client
    config._get_drive_item.return_value = mock_drive_item
    return config


@pytest.fixture
def mock_download_config():
    config = Mock(spec=SharepointDownloaderConfig)
    config.max_retries = 3
    return config


@pytest.fixture
def sharepoint_downloader(mock_connection_config, mock_download_config):
    downloader = SharepointDownloader(
        connection_config=mock_connection_config, download_config=mock_download_config
    )
    return downloader


@pytest.fixture
def file_data():
    return FileData(
        source_identifiers=SourceIdentifiers(
            filename="test.docx", fullpath="/sites/test/Shared Documents/test.docx"
        ),
        connector_type="sharepoint",
        identifier="test-id",
    )


def test_fetch_file(
    mock_client, mock_drive_item, mock_site, mock_file, sharepoint_downloader, file_data
):
    """Test successful file fetch without any errors"""
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.return_value = (
        mock_site
    )
    mock_drive_item.get_by_path.return_value.get.return_value.execute_query.return_value = mock_file
    result = sharepoint_downloader._fetch_file(file_data)

    assert result == mock_file
    assert mock_client.sites.get_by_url.return_value.get.return_value.execute_query.call_count == 1
    assert mock_drive_item.get_by_path.return_value.get.return_value.execute_query.call_count == 1
    mock_drive_item.get_by_path.assert_called_with("/sites/test/Shared Documents/test.docx")


def test_fetch_file_retries_on_429_error(
    mock_client, mock_drive_item, mock_site, sharepoint_downloader, file_data
):
    """Test that _fetch_file retries when encountering 429 errors"""
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.return_value = (
        mock_site
    )
    mock_drive_item.get_by_path.return_value.get.return_value.execute_query.side_effect = [
        Exception("429 Client Error"),
        Exception("Request has been throttled"),
        mock_file,
    ]

    result = sharepoint_downloader._fetch_file(file_data)
    assert result == mock_file
    assert mock_drive_item.get_by_path.return_value.get.return_value.execute_query.call_count == 3


def test_fetch_file_fails_after_max_retries(
    mock_client, mock_drive_item, mock_site, sharepoint_downloader, file_data
):
    """Test that _fetch_file fails after exhausting max retries"""
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.return_value = (
        mock_site
    )
    mock_drive_item.get_by_path.return_value.get.return_value.execute_query.side_effect = Exception(
        "429 Client Error"
    )

    with pytest.raises(Exception, match="429"):
        sharepoint_downloader._fetch_file(file_data)

    expected_calls = sharepoint_downloader.download_config.max_retries
    assert (
        mock_drive_item.get_by_path.return_value.get.return_value.execute_query.call_count
        == expected_calls
    )


def test_fetch_file_handles_site_not_found_immediately(
    mock_client, sharepoint_downloader, file_data
):
    """Test that site not found errors are not retried"""
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        Exception("Site not found")
    )

    with pytest.raises(SourceConnectionError, match="Site not found"):
        sharepoint_downloader._fetch_file(file_data)

    assert mock_client.sites.get_by_url.return_value.get.return_value.execute_query.call_count == 1


def _make_permission(raw_props: dict) -> Mock:
    perm = Mock()
    perm.properties = raw_props
    return perm


class TestExtractIdentityIdsFromRaw:
    def test_azure_ad_user_from_granted_to_v2(self):
        raw = {
            "grantedToV2": {
                "user": {"id": "user-uuid-1", "displayName": "Alice"},
            },
        }
        users, groups = SharepointIndexer._extract_identity_ids_from_raw(raw)
        assert users == {"user-uuid-1"}
        assert groups == set()

    def test_azure_ad_group_from_granted_to_v2(self):
        raw = {
            "grantedToV2": {
                "group": {"id": "group-uuid-1", "displayName": "Owners M365"},
            },
        }
        users, groups = SharepointIndexer._extract_identity_ids_from_raw(raw)
        assert users == set()
        assert groups == {"group-uuid-1"}

    def test_falls_back_to_granted_to_when_v2_missing(self):
        raw = {
            "grantedTo": {
                "user": {"id": "user-uuid-fallback"},
            },
        }
        users, groups = SharepointIndexer._extract_identity_ids_from_raw(raw)
        assert users == {"user-uuid-fallback"}

    def test_prefers_v2_over_granted_to(self):
        raw = {
            "grantedToV2": {"user": {"id": "v2-user"}},
            "grantedTo": {"user": {"id": "v1-user"}},
        }
        users, _ = SharepointIndexer._extract_identity_ids_from_raw(raw)
        assert users == {"v2-user"}

    def test_link_based_grants(self):
        raw = {
            "grantedToIdentitiesV2": [
                {"user": {"id": "link-user-1"}},
                {"group": {"id": "link-group-1"}},
            ],
        }
        users, groups = SharepointIndexer._extract_identity_ids_from_raw(raw)
        assert users == {"link-user-1"}
        assert groups == {"link-group-1"}

    def test_skips_entries_without_id(self):
        raw = {
            "grantedToV2": {
                "user": {"displayName": "No ID User"},
                "group": {"displayName": "No ID Group"},
            },
        }
        users, groups = SharepointIndexer._extract_identity_ids_from_raw(raw)
        assert users == set()
        assert groups == set()

    def test_empty_props_returns_empty(self):
        users, groups = SharepointIndexer._extract_identity_ids_from_raw({})
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
        users, groups = SharepointIndexer._extract_identity_ids_from_raw(raw)
        assert users == set()
        assert groups == set()

    def test_site_group_numeric_ids_ignored(self):
        """siteGroup entries lack Azure AD mapping and sit under a different key."""
        raw = {
            "grantedToV2": {
                "siteGroup": {"id": "3", "displayName": "Owners"},
            },
        }
        users, groups = SharepointIndexer._extract_identity_ids_from_raw(raw)
        assert users == set()
        assert groups == set()


class TestExtractPermissions:
    @pytest.fixture
    def indexer(self):
        indexer = Mock(spec=SharepointIndexer)
        indexer.extract_permissions = SharepointIndexer.extract_permissions.__get__(
            indexer, SharepointIndexer
        )
        indexer._extract_identity_ids_from_raw = SharepointIndexer._extract_identity_ids_from_raw
        return indexer

    def test_empty_permissions_returns_empty_dict_list(self, indexer):
        result = indexer.extract_permissions([])
        assert result == [{}]

    def test_owner_role_maps_to_all_operations(self, indexer):
        perm = _make_permission(
            {
                "roles": ["owner"],
                "grantedToV2": {"user": {"id": "owner-uuid"}},
            }
        )
        result = indexer.extract_permissions([perm])
        assert result == [
            {"read": {"users": ["owner-uuid"], "groups": []}},
            {"update": {"users": ["owner-uuid"], "groups": []}},
            {"delete": {"users": ["owner-uuid"], "groups": []}},
        ]

    def test_read_role_maps_to_read_only(self, indexer):
        perm = _make_permission(
            {
                "roles": ["read"],
                "grantedToV2": {"user": {"id": "reader-uuid"}},
            }
        )
        result = indexer.extract_permissions([perm])
        assert result == [
            {"read": {"users": ["reader-uuid"], "groups": []}},
            {"update": {"users": [], "groups": []}},
            {"delete": {"users": [], "groups": []}},
        ]

    def test_write_role_maps_to_read_and_update(self, indexer):
        perm = _make_permission(
            {
                "roles": ["write"],
                "grantedToV2": {"group": {"id": "writers-group"}},
            }
        )
        result = indexer.extract_permissions([perm])
        assert result == [
            {"read": {"users": [], "groups": ["writers-group"]}},
            {"update": {"users": [], "groups": ["writers-group"]}},
            {"delete": {"users": [], "groups": []}},
        ]

    def test_multiple_permissions_merged(self, indexer):
        perms = [
            _make_permission(
                {
                    "roles": ["owner"],
                    "grantedToV2": {"group": {"id": "owners-group"}},
                }
            ),
            _make_permission(
                {
                    "roles": ["write"],
                    "grantedToV2": {"user": {"id": "kevin-uuid"}},
                }
            ),
            _make_permission(
                {
                    "roles": ["read"],
                    "grantedToV2": {"user": {"id": "visitor-uuid"}},
                }
            ),
        ]
        result = indexer.extract_permissions(perms)
        assert result == [
            {"read": {"users": sorted(["kevin-uuid", "visitor-uuid"]), "groups": ["owners-group"]}},
            {"update": {"users": ["kevin-uuid"], "groups": ["owners-group"]}},
            {"delete": {"users": [], "groups": ["owners-group"]}},
        ]

    def test_site_group_only_permission_skipped(self, indexer):
        perm = _make_permission(
            {
                "roles": ["read"],
                "grantedToV2": {
                    "siteGroup": {"id": "4", "displayName": "Visitors"},
                },
            }
        )
        result = indexer.extract_permissions([perm])
        assert result == [
            {"read": {"users": [], "groups": []}},
            {"update": {"users": [], "groups": []}},
            {"delete": {"users": [], "groups": []}},
        ]

    def test_unmapped_role_logged_and_skipped(self, indexer):
        perm = _make_permission(
            {
                "roles": ["some_future_role"],
                "grantedToV2": {"user": {"id": "user-1"}},
            }
        )
        result = indexer.extract_permissions([perm])
        assert result == [
            {"read": {"users": [], "groups": []}},
            {"update": {"users": [], "groups": []}},
            {"delete": {"users": [], "groups": []}},
        ]

    def test_limited_access_role_produces_no_operations(self, indexer):
        perm = _make_permission(
            {
                "roles": ["sp.limited access"],
                "grantedToV2": {"user": {"id": "user-1"}},
            }
        )
        result = indexer.extract_permissions([perm])
        assert result == [
            {"read": {"users": [], "groups": []}},
            {"update": {"users": [], "groups": []}},
            {"delete": {"users": [], "groups": []}},
        ]

    def test_ids_are_sorted_for_deterministic_output(self, indexer):
        perms = [
            _make_permission(
                {
                    "roles": ["read"],
                    "grantedToV2": {"user": {"id": "zzz-user"}},
                }
            ),
            _make_permission(
                {
                    "roles": ["read"],
                    "grantedToV2": {"user": {"id": "aaa-user"}},
                }
            ),
        ]
        result = indexer.extract_permissions(perms)
        read_users = result[0]["read"]["users"]
        assert read_users == ["aaa-user", "zzz-user"]

    def test_output_matches_google_drive_schema(self, indexer):
        perm = _make_permission(
            {
                "roles": ["read"],
                "grantedToV2": {"user": {"id": "u1"}},
            }
        )
        result = indexer.extract_permissions([perm])
        assert len(result) == 3
        assert list(result[0].keys()) == ["read"]
        assert list(result[1].keys()) == ["update"]
        assert list(result[2].keys()) == ["delete"]
        for entry in result:
            val = list(entry.values())[0]
            assert set(val.keys()) == {"users", "groups"}
            assert isinstance(val["users"], list)
            assert isinstance(val["groups"], list)


def _make_drive_item(name="test.docx", permissions=None):
    drive_item = Mock()
    drive_item.name = name
    drive_item.parent_reference.path = "/drives/d1/root:"
    drive_item.last_modified_datetime = None
    drive_item.created_datetime = None
    drive_item.id = "item-id"
    drive_item.etag = "etag-1"
    drive_item.properties = {}
    if permissions is not None:
        drive_item.permissions = permissions
    return drive_item


def _make_indexer():
    conn = Mock(spec=SharepointConnectionConfig)
    conn.user_pname = "test@example.com"
    idx_config = Mock(spec=SharepointIndexerConfig)
    idx_config.path = ""
    return SharepointIndexer(connection_config=conn, index_config=idx_config)


class TestDriveItemToFileDataSync:
    def test_permissions_attached_when_pre_hydrated(self):
        indexer = _make_indexer()
        perm = Mock()
        perm.properties = {
            "roles": ["read"],
            "grantedToV2": {"user": {"id": "user-1"}},
        }
        drive_item = _make_drive_item(permissions=[perm])

        file_data = indexer.drive_item_to_file_data_sync(drive_item)
        assert file_data.metadata.permissions_data is not None
        assert len(file_data.metadata.permissions_data) == 3

    def test_permissions_none_when_not_hydrated(self):
        indexer = _make_indexer()
        drive_item = _make_drive_item(permissions=[])

        file_data = indexer.drive_item_to_file_data_sync(drive_item)
        assert file_data.metadata.permissions_data is None


class TestFetchPermissionsBatched:
    def test_batch_queues_and_flushes(self):
        from unstructured_ingest.processes.connectors.sharepoint import PERMISSIONS_BATCH_SIZE

        indexer = _make_indexer()
        client = Mock()
        items = [_make_drive_item(f"file{i}.docx") for i in range(3)]

        indexer._fetch_permissions_batched(client, items)

        for item in items:
            item.permissions.get.assert_called_once()
        client.execute_batch.assert_called_once_with(items_per_batch=PERMISSIONS_BATCH_SIZE)

    def test_falls_back_to_per_item_on_batch_failure(self):
        indexer = _make_indexer()
        client = Mock()
        client._queries = [object()]
        client.execute_batch.side_effect = Exception("batch failed")
        items = [_make_drive_item(f"file{i}.docx") for i in range(2)]

        indexer._fetch_permissions_batched(client, items)

        client.execute_batch.assert_called_once()
        assert client._queries == []
        for item in items:
            assert item.permissions.get.return_value.execute_query.called

    def test_fallback_works_without_internal_query_list(self):
        indexer = _make_indexer()

        class ClientWithoutInternalQueue:
            def __init__(self):
                self.execute_batch_calls = 0

            def execute_batch(self, **kwargs):
                self.execute_batch_calls += 1
                raise Exception("batch failed")

        client = ClientWithoutInternalQueue()
        items = [_make_drive_item(f"file{i}.docx") for i in range(2)]

        indexer._fetch_permissions_batched(client, items)

        assert client.execute_batch_calls == 1
        for item in items:
            assert item.permissions.get.return_value.execute_query.called

    def test_per_item_fallback_handles_individual_errors(self):
        from office365.runtime.client_request_exception import ClientRequestException

        indexer = _make_indexer()
        client = Mock()
        client._queries = []
        client.execute_batch.side_effect = Exception("batch failed")

        ok_item = _make_drive_item("ok.docx")
        fail_item = _make_drive_item("fail.docx")

        mock_response = Mock()
        mock_response.headers = {"Content-Type": "text/plain"}
        mock_response.content = b"Forbidden"
        mock_response.status_code = 403
        exc = ClientRequestException(response=mock_response)

        ok_item.permissions.get.return_value.execute_query.return_value = []
        fail_item.permissions.get.return_value.execute_query.side_effect = exc

        indexer._fetch_permissions_batched(client, [ok_item, fail_item])


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
