import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from pydantic import Secret

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import (
    NotFoundError,
    RateLimitError,
    SourceConnectionError,
    SourceConnectionNetworkError,
    UserAuthError,
    UserError,
    ValueError,
)
from unstructured_ingest.processes.connectors.onedrive import OnedriveIndexer
from unstructured_ingest.processes.connectors.sharepoint import (
    NON_INGESTIBLE_EXTENSIONS,
    SharepointAccessConfig,
    SharepointConnectionConfig,
    SharepointDownloader,
    SharepointDownloaderConfig,
    SharepointIndexer,
    SharepointIndexerConfig,
    _is_non_ingestible_artifact,
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
    with pytest.raises(UserAuthError) as exc_info:
        sharepoint_downloader._fetch_file(file_data)
    assert exc_info.value.status_code == 401
    # 401 is not retriable — one attempt only, no retry storm under auth misconfig.
    assert mock_client.sites.get_by_url.return_value.get.return_value.execute_query.call_count == 1


def test_fetch_file_surfaces_auth_error_on_403(mock_client, sharepoint_downloader, file_data):
    # 403 keeps the UserAuthError type (auth-class handling) but must pass through the real
    # HTTP 403 rather than collapsing to UserAuthError's class default of 401.
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(403)
    )
    with pytest.raises(UserAuthError) as exc_info:
        sharepoint_downloader._fetch_file(file_data)
    assert exc_info.value.status_code == 403
    assert "[HTTP 403]" in str(exc_info.value)


def test_fetch_file_surfaces_not_found_on_404(mock_client, sharepoint_downloader, file_data):
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(404)
    )
    with pytest.raises(NotFoundError) as exc_info:
        sharepoint_downloader._fetch_file(file_data)
    assert exc_info.value.status_code == 404


def test_fetch_file_surfaces_real_status_on_other_4xx(
    mock_client, sharepoint_downloader, file_data
):
    # An unmapped 4xx (e.g. 409 Conflict) falls to UserError, but must still report its real
    # status instead of UserError's class default (422) — "pass the status through at least".
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(409)
    )
    with pytest.raises(UserError) as exc_info:
        sharepoint_downloader._fetch_file(file_data)
    assert exc_info.value.status_code == 409
    assert "[HTTP 409]" in str(exc_info.value)


def test_fetch_file_includes_response_body_in_error(mock_client, sharepoint_downloader, file_data):
    # The upstream response body is passed through on the raised error, not just logged.
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(403, text="AccessDenied: app lacks Sites.Read.All")
    )
    with pytest.raises(UserAuthError) as exc_info:
        sharepoint_downloader._fetch_file(file_data)
    assert "AccessDenied: app lacks Sites.Read.All" in str(exc_info.value)


def test_fetch_file_truncates_long_response_body(mock_client, sharepoint_downloader, file_data):
    # A pathologically long body is truncated so the error message stays bounded.
    from unstructured_ingest.processes.connectors.sharepoint import _MAX_BODY_CHARS

    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(404, text="x" * (_MAX_BODY_CHARS + 100))
    )
    with pytest.raises(NotFoundError) as exc_info:
        sharepoint_downloader._fetch_file(file_data)
    message = str(exc_info.value)
    assert "x" * _MAX_BODY_CHARS in message
    assert "x" * (_MAX_BODY_CHARS + 1) not in message
    assert "…" in message


def test_fetch_file_404_on_file_attributes_to_file_not_site(
    mock_client, mock_drive_item, mock_site, sharepoint_downloader, file_data
):
    # A 404 on the file fetch must be attributed to the file, not the site: the site
    # resolved fine, the requested file is what's missing.
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.return_value = (
        mock_site
    )
    mock_drive_item.get_by_path.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(404)
    )
    with pytest.raises(NotFoundError) as exc_info:
        sharepoint_downloader._fetch_file(file_data)
    message = str(exc_info.value)
    assert "SharePoint file" in message
    assert "SharePoint site" not in message


def test_fetch_file_truncates_response_body_in_logs(
    mock_client, sharepoint_downloader, file_data, caplog
):
    # The logged body is capped too (not just the raised message) so a large upstream
    # payload isn't written unbounded — and on every retry attempt.
    import logging

    from unstructured_ingest.processes.connectors.sharepoint import _MAX_BODY_CHARS

    long_body = "x" * (_MAX_BODY_CHARS + 100)
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(403, text=long_body)
    )
    with caplog.at_level(logging.ERROR), pytest.raises(UserAuthError):
        sharepoint_downloader._fetch_file(file_data)
    assert "…" in caplog.text
    assert "x" * (_MAX_BODY_CHARS + 1) not in caplog.text


# A throttle's Retry-After is stamped on the typed error so the downloader's retry can
# honor the server's requested backoff (the retry sees the typed error, not the raw
# ClientRequestException). Tested on the mapper directly to avoid a real wait loop.
def test_handle_exception_stamps_retry_after_from_header():
    from unstructured_ingest.processes.connectors.sharepoint import (
        _handle_client_request_exception,
    )

    exc = _client_request_exception(429, headers={"Retry-After": "30"})
    with pytest.raises(RateLimitError) as exc_info:
        _handle_client_request_exception(exc, "SharePoint site x")
    assert exc_info.value.retry_after == 30.0


def test_handle_exception_no_retry_after_when_header_absent():
    from unstructured_ingest.processes.connectors.sharepoint import (
        _handle_client_request_exception,
    )

    exc = _client_request_exception(429)  # no Retry-After header
    with pytest.raises(RateLimitError) as exc_info:
        _handle_client_request_exception(exc, "SharePoint site x")
    assert getattr(exc_info.value, "retry_after", None) is None


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("30", 30.0),  # delta-seconds form (what Graph sends)
        ("0", 0.0),
        ("not-a-number", None),  # unparseable -> ignored
        (None, None),  # absent
    ],
)
def test_parse_retry_after(header, expected):
    from unstructured_ingest.processes.connectors.sharepoint import _parse_retry_after

    headers = {"Retry-After": header} if header is not None else {}
    assert _parse_retry_after(headers) == expected


def test_honor_retry_after_prefers_larger_server_backoff_capped():
    from unstructured_ingest.processes.connectors.sharepoint import (
        _MAX_RETRY_AFTER_WAIT,
        _honor_retry_after,
    )

    class _Exc(Exception):
        pass

    exc = _Exc()

    # Server backoff longer than exponential wins.
    exc.retry_after = 30.0
    assert _honor_retry_after(4.0, exc) == 30.0
    # Exponential wins when it's already longer than Retry-After.
    exc.retry_after = 1.0
    assert _honor_retry_after(4.0, exc) == 4.0
    # Pathologically large Retry-After is capped.
    exc.retry_after = 99999.0
    assert _honor_retry_after(4.0, exc) == _MAX_RETRY_AFTER_WAIT
    # No Retry-After stamped -> fall back to exponential.
    assert _honor_retry_after(4.0, _Exc()) == 4.0


def test_fetch_file_retries_then_raises_rate_limit_on_429(
    mock_client, sharepoint_downloader, file_data
):
    # A genuine 429 must both trigger retries and surface as RateLimitError — previously
    # it was rewritten to "Site not found" before the retry classifier could see the 429.
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = (
        _client_request_exception(429)
    )
    with pytest.raises(RateLimitError) as exc_info:
        sharepoint_downloader._fetch_file(file_data)
    assert exc_info.value.status_code == 429
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
    idx_config.team_id = None
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
    with pytest.raises(SourceConnectionNetworkError) as exc_info:
        sharepoint_downloader._fetch_file(file_data)
    # Real 5xx passes through instead of collapsing to the class default (400).
    assert exc_info.value.status_code == 503


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


def test_fetch_file_retriable_then_nonretriable_stops_on_nonretriable(
    mock_client, sharepoint_downloader, file_data
):
    # Mixed sequence: a retriable 503 followed by a non-retriable 403 must stop on the 403
    # (no further retries) and surface it with its real status — pins that the classifier
    # keys off each error's actual status, not a blanket "keep retrying".
    mock_client.sites.get_by_url.return_value.get.return_value.execute_query.side_effect = [
        _client_request_exception(503),
        _client_request_exception(403),
    ]
    with pytest.raises(UserAuthError) as exc_info:
        sharepoint_downloader._fetch_file(file_data)
    assert exc_info.value.status_code == 403
    # Exactly two attempts: 503 retried once, 403 halted retrying.
    assert mock_client.sites.get_by_url.return_value.get.return_value.execute_query.call_count == 2


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
    idx_config.team_id = None
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
    idx_config.team_id = None
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
    idx_config.team_id = None
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


def test_flush_missing_permission_entry_defaults_to_none():
    """SharepointIndexer._flush must pass raw_permissions=None for a drive item
    missing from the permission-fetch result (fetch unavailable -> skip digest),
    not [] (which would fabricate a revocation). A present entry still flows
    through unchanged. Mirrors OneDrive's perms_by_id.get(id) default.
    """
    conn = Mock(spec=SharepointConnectionConfig)
    conn.site = "https://test.sharepoint.com/sites/test"
    conn.get_token.return_value = {"access_token": "tok"}
    conn.get_client.return_value = Mock()
    conn._get_drive_item.return_value = Mock()

    idx_config = Mock(spec=SharepointIndexerConfig)
    idx_config.path = ""
    idx_config.recursive = False
    idx_config.team_id = None

    indexer = SharepointIndexer(connection_config=conn, index_config=idx_config)

    di_present = Mock()
    di_present.id = "present"
    di_missing = Mock()
    di_missing.id = "missing"

    captured: dict = {}

    async def _capture(drive_item, raw_permissions=None):
        captured[drive_item.id] = raw_permissions
        return Mock()

    with (
        patch.object(SharepointIndexer, "_get_target_drive_item") as mock_target,
        patch.object(
            indexer,
            "_fetch_permissions_raw",
            return_value={"present": [{"roles": ["read"]}]},  # "missing" omitted
        ),
        patch.object(indexer, "drive_item_to_file_data", new=AsyncMock(side_effect=_capture)),
    ):
        mock_target.return_value.get_files.return_value.execute_query.return_value = [
            di_present,
            di_missing,
        ]

        async def _drain():
            return [fd async for fd in indexer.run_async()]

        asyncio.run(_drain())

    assert captured["present"] == [{"roles": ["read"]}]
    assert captured["missing"] is None


# ---------------------------------------------------------------------------
# Teams channel files (team mode)
# ---------------------------------------------------------------------------


def _graph_response(status_code: int, json_body: dict = None, text: str = "") -> Mock:
    resp = Mock()
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    resp.text = text
    return resp


def _make_team_indexer(
    team_id: str = "team-1", channels=None, site=None, recursive: bool = False
) -> SharepointIndexer:
    conn = Mock(spec=SharepointConnectionConfig)
    conn.site = site
    conn.get_token.return_value = {"access_token": "tok"}
    idx_config = Mock(spec=SharepointIndexerConfig)
    idx_config.path = ""
    idx_config.recursive = recursive
    idx_config.team_id = team_id
    idx_config.channels = channels
    return SharepointIndexer(connection_config=conn, index_config=idx_config)


class TestTargetingValidation:
    def test_requires_at_least_one_mode(self):
        indexer = _make_team_indexer(team_id=None, site=None)
        with pytest.raises(UserError, match="required"):
            indexer._validate_targeting()

    def test_rejects_both_site_and_team(self):
        indexer = _make_team_indexer(
            team_id="team-1", site="https://test.sharepoint.com/sites/test"
        )
        with pytest.raises(UserError, match="not both"):
            indexer._validate_targeting()

    def test_team_only_is_valid(self):
        _make_team_indexer(team_id="team-1", site=None)._validate_targeting()

    def test_site_only_is_valid(self):
        _make_team_indexer(
            team_id=None, site="https://test.sharepoint.com/sites/test"
        )._validate_targeting()

    def test_is_team_mode_flag(self):
        assert _make_team_indexer(team_id="team-1", site=None)._is_team_mode() is True
        assert (
            _make_team_indexer(
                team_id=None, site="https://test.sharepoint.com/sites/test"
            )._is_team_mode()
            is False
        )


class TestGraphGet:
    def test_sets_prefer_and_auth_headers(self):
        indexer = _make_team_indexer()
        with patch("requests.get") as mock_get:
            indexer._graph_get("tok", "/teams/x/channels", prefer="include-unknown-enum-members")
        assert mock_get.call_args[0][0] == "https://graph.microsoft.com/v1.0/teams/x/channels"
        headers = mock_get.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer tok"
        assert headers["Prefer"] == "include-unknown-enum-members"

    def test_passes_absolute_url_through(self):
        indexer = _make_team_indexer()
        with patch("requests.get") as mock_get:
            indexer._graph_get("tok", "https://graph.microsoft.com/v1.0/next-page")
        assert mock_get.call_args[0][0] == "https://graph.microsoft.com/v1.0/next-page"
        assert "Prefer" not in mock_get.call_args.kwargs["headers"]

    def test_network_error_becomes_retriable_connection_error(self):
        # A raw requests network error/timeout must not escape uncaught (which would abort
        # an in-flight multi-channel crawl); it is a retriable typed connection error.
        import requests

        indexer = _make_team_indexer()
        with (
            patch("requests.get", side_effect=requests.exceptions.ConnectionError("boom")),
            pytest.raises(SourceConnectionNetworkError),
        ):
            indexer._graph_get("tok", "/teams/x/channels")


class TestListChannels:
    def test_sends_prefer_header_and_paginates(self):
        indexer = _make_team_indexer()
        page1 = _graph_response(
            200,
            {"value": [{"id": "c1"}], "@odata.nextLink": "https://graph.microsoft.com/v1.0/next"},
        )
        page2 = _graph_response(200, {"value": [{"id": "c2"}]})
        with patch.object(indexer, "_graph_get", side_effect=[page1, page2]) as mock_get:
            result = indexer._list_channels_sync("tok")
        assert [c["id"] for c in result] == ["c1", "c2"]
        # The channels enumeration must send the Prefer header (D6) or shared channels
        # are mistyped as unknownFutureValue.
        assert mock_get.call_args_list[0].kwargs.get("prefer") == "include-unknown-enum-members"

    def test_403_maps_to_user_auth_error(self):
        indexer = _make_team_indexer()
        with (
            patch.object(indexer, "_graph_get", return_value=_graph_response(403, text="denied")),
            pytest.raises(UserAuthError, match="Channel.ReadBasic.All"),
        ):
            indexer._list_channels_sync("tok")

    def test_404_maps_to_not_found(self):
        indexer = _make_team_indexer()
        with (
            patch.object(indexer, "_graph_get", return_value=_graph_response(404)),
            pytest.raises(NotFoundError, match="Team not found"),
        ):
            indexer._list_channels_sync("tok")

    def test_429_maps_to_rate_limit(self):
        indexer = _make_team_indexer()
        with (
            patch.object(indexer, "_graph_get", return_value=_graph_response(429)),
            pytest.raises(RateLimitError),
        ):
            indexer._list_channels_sync("tok")


class TestPrecheckTeam:
    def test_probe_403_raises_auth_error(self):
        # Missing Sites.Read.All while channel enumeration works must surface here, not as
        # an empty crawl later.
        indexer = _make_team_indexer()
        with (
            patch.object(indexer, "_graph_get", return_value=_graph_response(403, text="denied")),
            pytest.raises(UserAuthError, match="Sites.Read.All"),
        ):
            indexer._probe_files_read_scope_sync("tok")

    def test_probe_200_passes(self):
        indexer = _make_team_indexer()
        with patch.object(
            indexer, "_graph_get", return_value=_graph_response(200, {"id": "root"})
        ):
            indexer._probe_files_read_scope_sync("tok")  # no raise

    def test_probe_429_raises_rate_limit(self):
        indexer = _make_team_indexer()
        with (
            patch.object(indexer, "_graph_get", return_value=_graph_response(429)),
            pytest.raises(RateLimitError),
        ):
            indexer._probe_files_read_scope_sync("tok")

    def test_probe_non_auth_error_does_not_block(self, caplog):
        # A non-auth hiccup (e.g. odd 404 on the group drive) must not block a connection
        # already validated for enumeration; the run-time paths remain the backstop.
        import logging

        indexer = _make_team_indexer()
        with (
            patch.object(indexer, "_graph_get", return_value=_graph_response(404, text="no drive")),
            caplog.at_level(logging.WARNING),
        ):
            indexer._probe_files_read_scope_sync("tok")  # no raise
        assert "file-read scope" in caplog.text

    def test_precheck_runs_enumeration_then_read_scope_probe(self):
        # Enumeration succeeds (first _graph_get) but the file-read probe 403s (second) ->
        # precheck fails fast.
        indexer = _make_team_indexer()
        channels_resp = _graph_response(200, {"value": [{"id": "c1", "displayName": "General"}]})
        drive_resp = _graph_response(403, text="denied")
        with (
            patch.object(indexer, "_graph_get", side_effect=[channels_resp, drive_resp]),
            pytest.raises(UserAuthError, match="Sites.Read.All"),
        ):
            indexer._precheck_team()

    def test_precheck_success_when_both_probes_pass(self):
        indexer = _make_team_indexer()
        channels_resp = _graph_response(200, {"value": [{"id": "c1", "displayName": "General"}]})
        drive_resp = _graph_response(200, {"id": "root"})
        with patch.object(indexer, "_graph_get", side_effect=[channels_resp, drive_resp]):
            indexer._precheck_team()  # no raise


class TestChannelFilesFolder:
    def test_returns_drive_and_item_id(self):
        indexer = _make_team_indexer()
        body = {"id": "item1", "parentReference": {"driveId": "drive1"}}
        with patch.object(indexer, "_graph_get", return_value=_graph_response(200, body)):
            result = indexer._get_channel_files_folder_sync("tok", "c1", "General")
        assert result == {"drive_id": "drive1", "item_id": "item1"}

    def test_404_not_provisioned_is_skipped(self):
        # On-demand provisioning: filesFolder 404s until the Files tab is opened (D7).
        indexer = _make_team_indexer()
        with patch.object(
            indexer,
            "_graph_get",
            return_value=_graph_response(404, text="Folder location for this channel is not ready"),
        ):
            assert indexer._get_channel_files_folder_sync("tok", "c1", "General") is None

    def test_403_shared_channel_is_skipped(self):
        # A forbidden *shared* channel can legitimately be cross-tenant → skip, don't abort.
        indexer = _make_team_indexer()
        with patch.object(indexer, "_graph_get", return_value=_graph_response(403)):
            assert (
                indexer._get_channel_files_folder_sync(
                    "tok", "c1", "Shared", membership_type="shared"
                )
                is None
            )

    def test_403_standard_channel_raises_auth_error(self):
        # A 403 on a standard/private channel means a real scope gap (e.g. missing
        # Sites.Read.All). It must surface, not silently produce an empty crawl.
        indexer = _make_team_indexer()
        with (
            patch.object(indexer, "_graph_get", return_value=_graph_response(403, text="denied")),
            pytest.raises(UserAuthError, match="Sites.Read.All"),
        ):
            indexer._get_channel_files_folder_sync(
                "tok", "c1", "General", membership_type="standard"
            )

    def test_403_unknown_membership_raises_auth_error(self):
        # No membership type given → treat conservatively (not a known-safe shared channel).
        indexer = _make_team_indexer()
        with (
            patch.object(indexer, "_graph_get", return_value=_graph_response(401)),
            pytest.raises(UserAuthError),
        ):
            indexer._get_channel_files_folder_sync("tok", "c1", "General")

    def test_429_raises_rate_limit(self):
        # Throttling must be retriable (consistent with channel enumeration), not a
        # permanent per-channel skip that loses all of that channel's files.
        indexer = _make_team_indexer()
        with (
            patch.object(indexer, "_graph_get", return_value=_graph_response(429)),
            pytest.raises(RateLimitError),
        ):
            indexer._get_channel_files_folder_sync(
                "tok", "c1", "General", membership_type="standard"
            )

    def test_5xx_raises_connection_error(self):
        indexer = _make_team_indexer()
        with (
            patch.object(indexer, "_graph_get", return_value=_graph_response(503)),
            pytest.raises(SourceConnectionNetworkError),
        ):
            indexer._get_channel_files_folder_sync(
                "tok", "c1", "General", membership_type="private"
            )

    def test_missing_drive_id_is_skipped(self):
        indexer = _make_team_indexer()
        with patch.object(
            indexer, "_graph_get", return_value=_graph_response(200, {"id": "item1"})
        ):
            assert indexer._get_channel_files_folder_sync("tok", "c1", "General") is None


class TestFilterChannels:
    def test_no_filter_returns_all(self):
        indexer = _make_team_indexer(channels=None)
        channels = [{"id": "c1", "displayName": "General"}]
        assert indexer._filter_channels(channels) == channels

    def test_filters_by_name_and_id_and_warns_missing(self, caplog):
        import logging

        indexer = _make_team_indexer(channels=["General", "c2", "does-not-exist"])
        channels = [
            {"id": "c1", "displayName": "General"},
            {"id": "c2", "displayName": "Random"},
            {"id": "c3", "displayName": "Other"},
        ]
        with caplog.at_level(logging.WARNING):
            selected = indexer._filter_channels(channels)
        assert {c["id"] for c in selected} == {"c1", "c2"}
        assert "does-not-exist" in caplog.text


class TestRunAsyncTeamMode:
    def test_indexes_files_across_channels_and_skips_unprovisioned(self):
        indexer = _make_team_indexer(team_id="team-1")
        client = MagicMock()
        indexer.connection_config.get_client.return_value = client

        di1 = Mock()
        di1.id = "f1"
        di2 = Mock()
        di2.id = "f2"
        (
            client.drives.__getitem__.return_value.items.__getitem__.return_value.get_files.return_value.execute_query.return_value
        ) = [di1, di2]

        channels = [
            {"id": "chA", "displayName": "General", "membershipType": "standard"},
            {"id": "chB", "displayName": "Private", "membershipType": "private"},
        ]

        def _files_folder(access_token, channel_id, channel_name, membership_type=None):
            # chA resolves; chB is not provisioned -> skip
            return {"drive_id": "d1", "item_id": "root1"} if channel_id == "chA" else None

        async def _capture(drive_item, raw_permissions=None):
            return FileData(
                source_identifiers=SourceIdentifiers(
                    filename=drive_item.id, fullpath=drive_item.id
                ),
                connector_type="sharepoint",
                identifier=drive_item.id,
            )

        with (
            patch.object(indexer, "_list_channels_sync", return_value=channels),
            patch.object(indexer, "_get_channel_files_folder_sync", side_effect=_files_folder),
            patch.object(indexer, "_fetch_permissions_raw", return_value={"f1": None, "f2": None}),
            patch.object(indexer, "drive_item_to_file_data", new=AsyncMock(side_effect=_capture)),
        ):
            results = _drain_run_async(indexer)

        # Only chA's two files; chB skipped because its files folder isn't provisioned.
        assert [r.identifier for r in results] == ["f1", "f2"]

        # Pin the cross-site addressing: the provisioned channel's *own* resolved
        # drive_id/item_id must be used. With a bare MagicMock, client.drives[x].items[y]
        # returns the same chain for any x/y, so without these asserts the test would pass
        # even if _run_team_async ignored the channel's drive — the core private/shared
        # channel behavior this PR adds.
        client.drives.__getitem__.assert_called_once_with("d1")
        client.drives.__getitem__.return_value.items.__getitem__.assert_called_once_with("root1")

    def test_run_async_rejects_both_targets(self):
        indexer = _make_team_indexer(
            team_id="team-1", site="https://test.sharepoint.com/sites/test"
        )
        with pytest.raises(UserError, match="not both"):
            _drain_run_async(indexer)


class TestTeamRecordLocator:
    def test_drive_item_to_file_data_sync_adds_drive_and_item_id(self):
        indexer = _make_sharepoint_indexer()
        drive_item = _make_sharepoint_drive_item()
        file_data = indexer.drive_item_to_file_data_sync(drive_item)
        record_locator = file_data.metadata.record_locator
        assert record_locator["drive_id"] == "d1"
        assert record_locator["item_id"] == "item-test.docx"
        # Base keys are preserved for backward compatibility.
        assert record_locator["server_relative_path"]


class TestDownloaderDriveIdResolution:
    def _downloader_with_client(self, client, mock_download_config) -> SharepointDownloader:
        conn = Mock(spec=SharepointConnectionConfig)
        conn.site = None
        conn.get_client.return_value = client
        return SharepointDownloader(
            connection_config=conn, download_config=mock_download_config
        )

    def _file_data_with_drive_ref(self) -> FileData:
        fd = FileData(
            source_identifiers=SourceIdentifiers(filename="f.docx", fullpath="General/f.docx"),
            connector_type="sharepoint",
            identifier="i1",
        )
        fd.metadata.record_locator = {"drive_id": "d1", "item_id": "it1"}
        return fd

    def test_uses_drive_id_and_skips_site_resolution(self, mock_download_config):
        client = MagicMock()
        mock_file = Mock()
        (
            client.drives.__getitem__.return_value.items.__getitem__.return_value.get.return_value.execute_query.return_value
        ) = mock_file
        downloader = self._downloader_with_client(client, mock_download_config)

        result = downloader._fetch_file(self._file_data_with_drive_ref())

        assert result is mock_file
        # Drive-id resolution must not fall back to (or need) the configured site.
        client.sites.get_by_url.assert_not_called()

    def test_drive_id_404_maps_to_not_found(self, mock_download_config):
        client = MagicMock()
        (
            client.drives.__getitem__.return_value.items.__getitem__.return_value.get.return_value.execute_query.side_effect
        ) = _client_request_exception(404)
        downloader = self._downloader_with_client(client, mock_download_config)

        with pytest.raises(NotFoundError) as exc_info:
            downloader._fetch_file(self._file_data_with_drive_ref())
        assert exc_info.value.status_code == 404

    def test_drive_id_403_maps_to_user_auth_error(self, mock_download_config):
        # 403 through the new drive-id branch must surface the real status (not a masked
        # SourceConnectionError) and must not retry — auth misconfig isn't transient.
        client = MagicMock()
        execute_query = (
            client.drives.__getitem__.return_value.items.__getitem__.return_value.get.return_value.execute_query
        )
        execute_query.side_effect = _client_request_exception(403)
        downloader = self._downloader_with_client(client, mock_download_config)

        with pytest.raises(UserAuthError) as exc_info:
            downloader._fetch_file(self._file_data_with_drive_ref())
        assert exc_info.value.status_code == 403
        assert execute_query.call_count == 1

    def test_drive_id_429_retries_then_raises_rate_limit(self, mock_download_config):
        # A throttle through the drive-id branch must retry up to max_retries and then
        # surface RateLimitError (429), matching the site-path branch's behavior.
        client = MagicMock()
        execute_query = (
            client.drives.__getitem__.return_value.items.__getitem__.return_value.get.return_value.execute_query
        )
        execute_query.side_effect = _client_request_exception(429)
        downloader = self._downloader_with_client(client, mock_download_config)

        with pytest.raises(RateLimitError) as exc_info:
            downloader._fetch_file(self._file_data_with_drive_ref())
        assert exc_info.value.status_code == 429
        assert execute_query.call_count == mock_download_config.max_retries

    def test_legacy_fallback_resolves_via_site_and_path(self, mock_download_config):
        # FileData indexed before drive_id was captured (no drive_id in record_locator)
        # must still resolve via the configured site + server-relative path.
        client = MagicMock()
        mock_site = Mock()
        mock_drive_item = Mock()
        mock_file = Mock()
        client.sites.get_by_url.return_value.get.return_value.execute_query.return_value = mock_site
        mock_drive_item.get_by_path.return_value.get.return_value.execute_query.return_value = (
            mock_file
        )
        conn = Mock(spec=SharepointConnectionConfig)
        conn.site = "https://test.sharepoint.com/sites/test"
        conn.get_client.return_value = client
        conn._get_drive_item.return_value = mock_drive_item
        downloader = SharepointDownloader(
            connection_config=conn, download_config=mock_download_config
        )
        fd = FileData(
            source_identifiers=SourceIdentifiers(
                filename="f.docx", fullpath="/sites/test/Shared Documents/f.docx"
            ),
            connector_type="sharepoint",
            identifier="i1",
        )  # no drive_id in record_locator -> legacy site+path fallback

        result = downloader._fetch_file(fd)

        assert result is mock_file
        mock_drive_item.get_by_path.assert_called_with("/sites/test/Shared Documents/f.docx")
        # The drive-id branch must not be used when no drive ref is present.
        client.drives.__getitem__.assert_not_called()


# ---------------------------------------------------------------------------
# Non-ingestible collaborative artifacts (Loop / Fluid / Whiteboard)
# ---------------------------------------------------------------------------


class TestNonIngestibleArtifactPredicate:
    def test_constant_is_the_fluid_family(self):
        assert NON_INGESTIBLE_EXTENSIONS == (".loop", ".fluid", ".whiteboard")

    @pytest.mark.parametrize(
        "name",
        [
            "notes.loop",
            "legacy.fluid",
            "board.whiteboard",
            "NOTES.LOOP",
            "Legacy.Fluid",
            "Board.WhiteBoard",
        ],
    )
    def test_matches_artifacts_case_insensitively(self, name):
        assert _is_non_ingestible_artifact(name) is True

    @pytest.mark.parametrize(
        "name",
        [
            "report.pdf",
            "data.docx",
            "notes.txt",
            "report.loop.pdf",  # only the final suffix counts
            "loopy.pptx",
            "loop",  # bare word, no extension
            "",
        ],
    )
    def test_rejects_normal_files(self, name):
        assert _is_non_ingestible_artifact(name) is False

    def test_non_string_is_false(self):
        # Guards against a test/mock object whose truthy auto-attribute would otherwise
        # match and silently drop a real drive item.
        assert _is_non_ingestible_artifact(None) is False
        assert _is_non_ingestible_artifact(Mock()) is False


class TestNonIngestibleArtifactFiltering:
    """Both the site and Teams crawls must drop Loop/Fluid/Whiteboard artifacts before
    they are permission-fetched or downloaded (shared `_emit_drive_items` chokepoint)."""

    @staticmethod
    def _named_item(name: str) -> Mock:
        di = Mock()
        di.id = name
        di.name = name
        return di

    @staticmethod
    def _perms_for_chunk(chunk, access_token):
        return {di.id: None for di in chunk}

    def test_team_mode_skips_loop_fluid_whiteboard(self):
        indexer = _make_team_indexer(team_id="team-1")
        client = MagicMock()
        indexer.connection_config.get_client.return_value = client

        items = [
            self._named_item("doc.pdf"),
            self._named_item("notes.loop"),
            self._named_item("legacy.fluid"),
            self._named_item("board.whiteboard"),
            self._named_item("sheet.xlsx"),
        ]
        (
            client.drives.__getitem__.return_value.items.__getitem__.return_value.get_files.return_value.execute_query.return_value
        ) = items

        channels = [{"id": "chA", "displayName": "General", "membershipType": "standard"}]

        async def _capture(drive_item, raw_permissions=None):
            return FileData(
                source_identifiers=SourceIdentifiers(
                    filename=drive_item.id, fullpath=drive_item.id
                ),
                connector_type="sharepoint",
                identifier=drive_item.id,
            )

        with (
            patch.object(indexer, "_list_channels_sync", return_value=channels),
            patch.object(
                indexer,
                "_get_channel_files_folder_sync",
                return_value={"drive_id": "d1", "item_id": "root1"},
            ),
            patch.object(indexer, "_fetch_permissions_raw", side_effect=self._perms_for_chunk),
            patch.object(indexer, "drive_item_to_file_data", new=AsyncMock(side_effect=_capture)),
        ):
            results = _drain_run_async(indexer)

        assert [r.identifier for r in results] == ["doc.pdf", "sheet.xlsx"]

    def test_site_mode_skips_loop_fluid_whiteboard(self):
        conn = Mock(spec=SharepointConnectionConfig)
        conn.site = "https://test.sharepoint.com/sites/test"
        conn.get_token.return_value = {"access_token": "tok"}
        conn.get_client.return_value = Mock()
        conn._get_drive_item.return_value = Mock()

        idx_config = Mock(spec=SharepointIndexerConfig)
        idx_config.path = ""
        idx_config.recursive = False
        idx_config.team_id = None

        indexer = SharepointIndexer(connection_config=conn, index_config=idx_config)

        items = [
            self._named_item("doc.pdf"),
            self._named_item("notes.loop"),
            self._named_item("legacy.fluid"),
            self._named_item("board.whiteboard"),
        ]

        captured: list = []

        async def _capture(drive_item, raw_permissions=None):
            captured.append(drive_item.id)
            return Mock()

        with (
            patch.object(SharepointIndexer, "_get_target_drive_item") as mock_target,
            patch.object(indexer, "_fetch_permissions_raw", side_effect=self._perms_for_chunk),
            patch.object(indexer, "drive_item_to_file_data", new=AsyncMock(side_effect=_capture)),
        ):
            mock_target.return_value.get_files.return_value.execute_query.return_value = items
            _drain_run_async(indexer)

        assert captured == ["doc.pdf"]
