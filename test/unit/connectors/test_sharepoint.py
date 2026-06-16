from unittest.mock import Mock, patch

import pytest
from pydantic import Secret

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import SourceConnectionError, ValueError
from unstructured_ingest.processes.connectors.onedrive import OnedriveIndexer
from unstructured_ingest.processes.connectors.sharepoint import (
    SharepointAccessConfig,
    SharepointConnectionConfig,
    SharepointDownloader,
    SharepointDownloaderConfig,
    SharepointIndexer,
    SharepointIndexerConfig,
)


class TestSharepointAccessConfig:
    def test_client_cred_only(self):
        config = SharepointAccessConfig(client_cred="secret-value")
        assert config.client_cred == "secret-value"
        assert config.oauth_token is None

    def test_oauth_token_only(self):
        config = SharepointAccessConfig(oauth_token="ey.access.token")
        assert config.oauth_token == "ey.access.token"
        assert config.client_cred is None

    def test_no_auth_raises_error(self):
        with pytest.raises(ValueError, match="must be set"):
            SharepointAccessConfig()

    def test_oauth_and_client_cred_raises_error(self):
        with pytest.raises(ValueError, match="cannot use both"):
            SharepointAccessConfig(
                client_cred="secret-value",
                oauth_token="ey.access.token",
            )

    def test_oauth_and_password_raises_error(self):
        with pytest.raises(ValueError, match="cannot use both"):
            SharepointAccessConfig(
                password="user-password",
                oauth_token="ey.access.token",
            )

    def test_empty_oauth_token_treated_as_missing(self):
        # validator and runtime both use truthiness; pin that consistency
        with pytest.raises(ValueError, match="must be set"):
            SharepointAccessConfig(oauth_token="")


class TestSharepointConnectionConfig:
    """Pins that the cross-field auth validator inherited from
    OnedriveConnectionConfig still applies on the SharePoint side."""

    def test_client_cred_without_client_id_raises(self):
        # client_cred auth needs client_id; reject at config time so users
        # don't hit cryptic AADSTS / MSAL errors at runtime
        with pytest.raises(ValueError, match="client_id is required"):
            SharepointConnectionConfig(
                site="https://contoso.sharepoint.com/sites/acme",
                user_pname="alice@contoso.com",
                tenant="tenant-id",
                access_config=Secret(SharepointAccessConfig(client_cred="secret-value")),
            )

    def test_oauth_token_without_client_id_succeeds(self):
        config = SharepointConnectionConfig(
            site="https://contoso.sharepoint.com/sites/acme",
            user_pname="alice@contoso.com",
            tenant="tenant-id",
            access_config=Secret(SharepointAccessConfig(oauth_token="ey.access.token")),
        )
        assert config.client_id is None


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
    # site-not-found is not retriable
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        Exception("Site not found")
    )

    with pytest.raises(SourceConnectionError, match="Site not found"):
        sharepoint_downloader._fetch_file(file_data)

    assert mock_client.sites.get_by_url.return_value.get.return_value.execute_query.call_count == 1


# Full coverage for the permission machinery lives in test_onedrive.py since
# the implementation is on OnedriveIndexer; the tests below pin the inheritance
# contract so any future SharePoint-side override has to come with real
# SharePoint coverage.


def _make_sharepoint_drive_item(name: str = "test.docx") -> Mock:
    drive_item = Mock()
    drive_item.name = name
    drive_item.id = f"item-{name}"
    drive_item.parent_reference.path = "/drives/d1/root:"
    # office365-rest-python-client exposes this as camelCase `driveId`
    drive_item.parent_reference.driveId = "d1"
    drive_item.last_modified_datetime = None
    drive_item.created_datetime = None
    drive_item.etag = "etag-1"
    drive_item.properties = {}
    return drive_item


def _make_sharepoint_indexer() -> SharepointIndexer:
    conn = Mock(spec=SharepointConnectionConfig)
    conn.user_pname = "test@example.com"
    conn.site = "https://test.sharepoint.com/sites/test"
    idx_config = Mock(spec=SharepointIndexerConfig)
    idx_config.path = ""
    return SharepointIndexer(connection_config=conn, index_config=idx_config)


class TestSharepointInheritsPermissionMachinery:
    def test_inheritance_identity_for_shared_methods(self):
        # same function object on both classes -> SharePoint runs identical code;
        # if anyone overrides on SharepointIndexer this fails and forces them to
        # add SharePoint-specific coverage
        assert (
            SharepointIndexer.extract_permissions
            is OnedriveIndexer.extract_permissions
        )
        assert (
            SharepointIndexer._fetch_permissions_raw
            is OnedriveIndexer._fetch_permissions_raw
        )
        assert (
            SharepointIndexer._extract_identity_ids_from_raw
            is OnedriveIndexer._extract_identity_ids_from_raw
        )
        assert (
            SharepointIndexer._parse_batch_response
            is OnedriveIndexer._parse_batch_response
        )

    def test_extract_permissions_owner_role_smoke(self):
        indexer = _make_sharepoint_indexer()
        result = indexer.extract_permissions(
            [{"roles": ["owner"], "grantedToV2": {"user": {"id": "user-1"}}}]
        )
        assert result == [
            {"read": {"users": ["user-1"], "groups": []}},
            {"update": {"users": ["user-1"], "groups": []}},
            {"delete": {"users": ["user-1"], "groups": []}},
        ]

    def test_extract_identity_ids_from_raw_smoke(self):
        users, groups = SharepointIndexer._extract_identity_ids_from_raw(
            {"grantedToV2": {"user": {"id": "u-1"}, "group": {"id": "g-1"}}}
        )
        assert users == {"u-1"}
        assert groups == {"g-1"}

    def test_drive_item_to_file_data_sync_wires_permissions(self):
        indexer = _make_sharepoint_indexer()
        drive_item = _make_sharepoint_drive_item()
        file_data = indexer.drive_item_to_file_data_sync(
            drive_item,
            raw_permissions=[
                {"roles": ["read"], "grantedToV2": {"user": {"id": "u-1"}}}
            ],
        )
        assert file_data.metadata.permissions_data is not None
        assert file_data.metadata.permissions_data[0]["read"]["users"] == ["u-1"]

    def test_fetch_permissions_raw_hits_graph_batch_endpoint(self):
        indexer = _make_sharepoint_indexer()
        items = [_make_sharepoint_drive_item("a.docx")]

        body = Mock()
        body.status_code = 200
        body.json.return_value = {
            "responses": [
                {
                    "id": "0",
                    "status": 200,
                    "body": {
                        "value": [
                            {"roles": ["read"], "grantedToV2": {"user": {"id": "u-1"}}}
                        ]
                    },
                }
            ]
        }

        with patch("requests.post", return_value=body) as mock_post:
            result = indexer._fetch_permissions_raw(items, access_token="tok")

        mock_post.assert_called_once()
        assert mock_post.call_args[0][0] == "https://graph.microsoft.com/v1.0/$batch"
        assert mock_post.call_args[1]["headers"]["Authorization"] == "Bearer tok"
        assert result["item-a.docx"][0]["grantedToV2"]["user"]["id"] == "u-1"
