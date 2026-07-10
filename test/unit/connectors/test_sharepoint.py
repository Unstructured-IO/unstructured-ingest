from unittest.mock import Mock, patch

import pytest
from pydantic import Secret

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import (
    NotFoundError,
    RateLimitError,
    SourceConnectionError,
    SourceConnectionNetworkError,
    UserAuthError,
    ValueError,
)
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


def _client_request_exception(status_code, text="upstream error", headers=None):
    """Build a ClientRequestException carrying a real HTTP status, as office365 raises.

    ClientRequestException subclasses requests.RequestException, whose __init__ reads
    ``response.headers``/``response.content`` — so we pass a response object, not a message.
    """
    from office365.runtime.client_request_exception import ClientRequestException

    response = Mock()
    response.headers = headers or {}
    response.content = b""
    response.status_code = status_code
    response.text = text
    return ClientRequestException(response=response)


# Regression: the downloader used to catch every ClientRequestException and re-raise
# it as SourceConnectionError("Site not found"), discarding the real HTTP status —
# so 401/403/404/429 all surfaced identically as "400: ... Site not found", and the
# retry classifier (matching the rewritten string) never fired for genuine throttles.
# These pin that the real status is now surfaced (and 429s retry).


def test_fetch_file_surfaces_auth_error_on_401(mock_client, sharepoint_downloader, file_data):
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(401)
    )
    with pytest.raises(UserAuthError):
        sharepoint_downloader._fetch_file(file_data)
    # 401 is not retriable — one attempt only, no retry storm under auth misconfig.
    assert mock_client.sites.get_by_url.return_value.get.return_value.execute_query.call_count == 1


def test_fetch_file_surfaces_auth_error_on_403(mock_client, sharepoint_downloader, file_data):
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(403)
    )
    with pytest.raises(UserAuthError):
        sharepoint_downloader._fetch_file(file_data)


def test_fetch_file_surfaces_not_found_on_404(mock_client, sharepoint_downloader, file_data):
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(404)
    )
    with pytest.raises(NotFoundError):
        sharepoint_downloader._fetch_file(file_data)


def test_fetch_file_retries_then_raises_rate_limit_on_429(
    mock_client, sharepoint_downloader, file_data
):
    # A genuine 429 must both trigger retries and surface as RateLimitError — previously
    # it was rewritten to "Site not found" before the retry classifier could see the 429.
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(429)
    )
    with pytest.raises(RateLimitError):
        sharepoint_downloader._fetch_file(file_data)
    assert (
        mock_client.sites.get_by_url.return_value.get.return_value.execute_query.call_count
        == sharepoint_downloader.download_config.max_retries
    )


def test_fetch_file_preserves_http_status_and_headers_in_logs(
    mock_client, sharepoint_downloader, file_data, caplog
):
    # Core AC: the real status/text + MS correlation headers must be captured before any
    # label, so the next occurrence attributes to a real HTTP condition.
    import logging

    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(
            403,
            text="Access forbidden for app",
            headers={"x-ms-ags-diagnostic": "diag-xyz", "request-id": "req-123"},
        )
    )
    with caplog.at_level(logging.ERROR), pytest.raises(UserAuthError):
        sharepoint_downloader._fetch_file(file_data)
    assert "403" in caplog.text
    assert "diag-xyz" in caplog.text


def _indexer_with_site_error(exc) -> SharepointIndexer:
    conn = Mock(spec=SharepointConnectionConfig)
    conn.site = "https://test.sharepoint.com/sites/test"
    conn.get_token.return_value = {"access_token": "tok"}
    conn.get_client.return_value.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (  # noqa: E501
        exc
    )
    idx_config = Mock(spec=SharepointIndexerConfig)
    idx_config.path = ""
    return SharepointIndexer(connection_config=conn, index_config=idx_config)


def _drain_run_async(indexer: SharepointIndexer) -> list:
    import asyncio

    async def _drain() -> list:
        return [fd async for fd in indexer.run_async()]

    return asyncio.run(_drain())


# The indexer's async run path resolves the same site as the downloader, so it must map
# upstream HTTP status the same way (shared helper) rather than masking as a generic
# connection error — otherwise index-time auth/throttle failures are misclassified.


def test_fetch_file_maps_5xx_to_connection_error_not_user_error(
    mock_client, sharepoint_downloader, file_data
):
    # A transient upstream 5xx must stay a connection-class error (retriable semantics),
    # not be reclassified as a non-retriable UserError.
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(503)
    )
    with pytest.raises(SourceConnectionNetworkError):
        sharepoint_downloader._fetch_file(file_data)


def test_fetch_file_retries_5xx_then_raises_connection_error(
    mock_client, sharepoint_downloader, file_data
):
    # A transient upstream 5xx (503) is retriable (parity with OneDrive's 429/503 set):
    # it must both retry up to max_retries and finally surface as SourceConnectionNetworkError.
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(503)
    )
    with pytest.raises(SourceConnectionNetworkError):
        sharepoint_downloader._fetch_file(file_data)
    assert (
        mock_client.sites.get_by_url.return_value.get.return_value.execute_query.call_count
        == sharepoint_downloader.download_config.max_retries
    )


def test_fetch_file_wraps_unknown_exception_as_connection_error(
    mock_client, sharepoint_downloader, file_data
):
    # A non-HTTP / unrecognized failure still surfaces as a connection error — this pins the
    # inline catch-all that replaced the removed @SourceConnectionNetworkError.wrap decorator.
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        RuntimeError("boom")
    )
    with pytest.raises(SourceConnectionNetworkError):
        sharepoint_downloader._fetch_file(file_data)


def test_indexer_run_async_surfaces_auth_error_on_401():
    indexer = _indexer_with_site_error(_client_request_exception(401))
    with pytest.raises(UserAuthError):
        _drain_run_async(indexer)


def test_indexer_run_async_surfaces_rate_limit_on_429():
    indexer = _indexer_with_site_error(_client_request_exception(429))
    with pytest.raises(RateLimitError):
        _drain_run_async(indexer)


def test_indexer_run_async_maps_path_resolution_error():
    # Site resolves, but the configured (non-root) path 404s during resolution.
    conn = Mock(spec=SharepointConnectionConfig)
    conn.site = "https://test.sharepoint.com/sites/test"
    conn.get_token.return_value = {"access_token": "tok"}
    site_drive_item = Mock()
    conn._get_drive_item.return_value = site_drive_item
    conn.get_client.return_value.sites.get_by_url.return_value.get.return_value.execute_query.return_value = Mock()  # noqa: E501
    site_drive_item.get_by_path.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(404)
    )
    idx_config = Mock(spec=SharepointIndexerConfig)
    idx_config.path = "Shared Documents/Subfolder"
    indexer = SharepointIndexer(connection_config=conn, index_config=idx_config)
    with pytest.raises(NotFoundError):
        _drain_run_async(indexer)


def test_indexer_run_async_maps_file_listing_error():
    # Site + path resolve, but listing files throttles.
    conn = Mock(spec=SharepointConnectionConfig)
    conn.site = "https://test.sharepoint.com/sites/test"
    conn.get_token.return_value = {"access_token": "tok"}
    site_drive_item = Mock()
    conn._get_drive_item.return_value = site_drive_item
    conn.get_client.return_value.sites.get_by_url.return_value.get.return_value.execute_query.return_value = Mock()  # noqa: E501
    site_drive_item.get_files.return_value.execute_query.side_effect = _client_request_exception(
        429
    )
    idx_config = Mock(spec=SharepointIndexerConfig)
    idx_config.path = ""  # root -> target drive item is the site drive item
    idx_config.recursive = False
    indexer = SharepointIndexer(connection_config=conn, index_config=idx_config)
    with pytest.raises(RateLimitError):
        _drain_run_async(indexer)


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
        assert SharepointIndexer.extract_permissions is OnedriveIndexer.extract_permissions
        assert SharepointIndexer._fetch_permissions_raw is OnedriveIndexer._fetch_permissions_raw
        assert (
            SharepointIndexer._extract_identity_ids_from_raw
            is OnedriveIndexer._extract_identity_ids_from_raw
        )
        assert SharepointIndexer._parse_batch_response is OnedriveIndexer._parse_batch_response

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
            raw_permissions=[{"roles": ["read"], "grantedToV2": {"user": {"id": "u-1"}}}],
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
                        "value": [{"roles": ["read"], "grantedToV2": {"user": {"id": "u-1"}}}]
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
