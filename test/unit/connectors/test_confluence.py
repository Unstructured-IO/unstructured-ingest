from collections import OrderedDict
from unittest import mock

import pytest
from dateutil import parser

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.confluence import (
    ConfluenceAccessConfig,
    ConfluenceConnectionConfig,
    ConfluenceDownloader,
    ConfluenceDownloaderConfig,
    ConfluenceIndexer,
    ConfluenceIndexerConfig,
    get_permissions_data,
)
from unstructured_ingest.utils.acl import compute_permissions_version


@pytest.fixture
def connection_config():
    """Provides a minimal ConfluenceConnectionConfig for testing."""
    access_config = ConfluenceAccessConfig(api_token="token")
    return ConfluenceConnectionConfig(
        url="https://dummy",
        username="user",
        access_config=access_config,
    )


def test_connection_config_multiple_auth():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(
                password="password",
                token="access_token",
            ),
            username="user_email",
            url="url",
        )


def test_connection_config_multiple_auth2():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(
                api_token="api_token",
                token="access_token",
            ),
            username="user_email",
            url="url",
        )


def test_connection_config_multiple_auth3():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(
                api_token="api_token",
                password="password",
            ),
            username="user_email",
            url="url",
        )


def test_connection_config_no_auth():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(access_config=ConfluenceAccessConfig(), url="url")


def test_connection_config_password_auth():
    ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(password="password"),
        url="url",
        username="user_email",
    )


def test_connection_config_api_token_auth():
    ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(api_token="api_token"),
        url="url",
        username="user_email",
    )


def test_connection_config_pat_auth():
    ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(token="access_token"),
        url="url",
    )


def test_connection_config_oauth_auth_requires_cloud_id():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(oauth_token="oauth_token"),
            url="https://example.atlassian.net/wiki",
            cloud=True,
        )


def test_connection_config_oauth_auth():
    config = ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(
            oauth_token="oauth_token",
            refresh_token="refresh_token",
        ),
        cloud_id="cloud-123",
        url="https://example.atlassian.net/wiki",
        cloud=True,
    )

    assert config.api_url() == "https://api.atlassian.com/ex/confluence/cloud-123/wiki"
    assert config.page_url("456") == "https://example.atlassian.net/wiki/pages/456"


def test_connection_config_oauth_auth_is_exclusive():
    with pytest.raises(ValueError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(
                api_token="api_token",
                oauth_token="oauth_token",
            ),
            cloud_id="cloud-123",
            username="user_email",
            url="https://example.atlassian.net/wiki",
            cloud=True,
        )


def test_indexer_oauth_file_data_uses_cloud_identity():
    config = ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(oauth_token="oauth_token"),
        cloud_id="cloud-123",
        url="https://example.atlassian.net/wiki",
        cloud=True,
    )
    indexer = ConfluenceIndexer(
        connection_config=config,
        index_config=ConfluenceIndexerConfig(spaces=["ENG"]),
    )
    mock_client = mock.MagicMock()
    mock_client.get.side_effect = [
        {"results": [{"id": 987, "key": "ENG"}]},
        {
            "results": [
                {
                    "id": "456",
                    "createdAt": "2026-05-28T10:00:00Z",
                    "version": {"createdAt": "2026-05-28T11:00:00Z", "number": 7},
                }
            ]
        },
    ]

    with mock.patch.object(type(config), "get_client", mock.MagicMock()):
        type(config).get_client.return_value.__enter__.return_value = mock_client

        file_data = next(indexer.run())

    assert file_data.identifier == "456"
    assert file_data.source_identifiers.fullpath == "cloud-123/ENG/456.html"
    assert file_data.metadata.url == "https://example.atlassian.net/wiki/pages/456"
    assert file_data.metadata.date_created == str(parser.parse("2026-05-28T10:00:00Z").timestamp())
    assert file_data.metadata.date_modified == str(parser.parse("2026-05-28T11:00:00Z").timestamp())
    assert file_data.metadata.version == "7"
    assert file_data.metadata.record_locator["cloud_id"] == "cloud-123"
    assert file_data.additional_metadata["site_url"] == "https://example.atlassian.net/wiki"
    mock_client.get.assert_has_calls(
        [
            mock.call("api/v2/spaces", params={"limit": 1, "keys": ["ENG"]}),
            mock.call("api/v2/pages", params={"space-id": 987, "limit": 100}),
        ],
        any_order=False,
    )


def test_get_space_by_key_falls_back_to_personal_space_alias(connection_config):
    indexer = ConfluenceIndexer(
        connection_config=connection_config,
        index_config=ConfluenceIndexerConfig(spaces=["~user-personal-space"]),
    )
    mock_client = mock.MagicMock()
    mock_client.get.side_effect = [
        {"results": []},
        {"results": [{"id": 987, "key": "generated-key", "alias": "~user-personal-space"}]},
    ]

    assert indexer._get_space_by_key(mock_client, "~user-personal-space") == {
        "id": 987,
        "key": "generated-key",
        "alias": "~user-personal-space",
    }
    mock_client.get.assert_has_calls(
        [
            mock.call("api/v2/spaces", params={"limit": 1, "keys": ["~user-personal-space"]}),
            mock.call(
                "api/v2/spaces",
                params={"limit": 250, "type": "personal", "status": "current"},
            ),
        ],
        any_order=False,
    )


def test_list_spaces_paginates_until_configured_limit(connection_config):
    indexer = ConfluenceIndexer(
        connection_config=connection_config,
        index_config=ConfluenceIndexerConfig(max_num_of_spaces=251),
    )
    mock_client = mock.MagicMock()
    mock_client.get.side_effect = [
        {
            "results": [{"id": i, "key": f"SPACE-{i}"} for i in range(250)],
            "_links": {"next": "/wiki/api/v2/spaces?cursor=next"},
        },
        {"results": [{"id": 250, "key": "SPACE-250"}]},
    ]

    spaces = indexer._list_spaces(mock_client)

    assert len(spaces) == 251
    assert spaces[-1] == {"id": 250, "key": "SPACE-250"}
    mock_client.get.assert_has_calls(
        [
            mock.call("api/v2/spaces", params={"limit": 250}),
            mock.call("api/v2/spaces?cursor=next", params=None),
        ],
        any_order=False,
    )


def test_get_docs_ids_within_one_space_uses_v2_pages(connection_config):
    indexer = ConfluenceIndexer(
        connection_config=connection_config,
        index_config=ConfluenceIndexerConfig(max_num_of_docs_from_each_space=2),
    )
    mock_client = mock.MagicMock()
    mock_client.get.return_value = {"results": [{"id": "1"}, {"id": "2"}, {"id": "3"}]}

    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client

        doc_ids = indexer._get_docs_ids_within_one_space(987)

    assert doc_ids == [
        {
            "space_id": 987,
            "doc_id": "1",
            "date_created": None,
            "date_modified": None,
            "version_number": None,
        },
        {
            "space_id": 987,
            "doc_id": "2",
            "date_created": None,
            "date_modified": None,
            "version_number": None,
        },
    ]
    mock_client.get.assert_called_once_with(
        "api/v2/pages",
        params={"space-id": 987, "limit": 2},
    )


def test_get_docs_ids_within_one_space_paginates_until_configured_limit(connection_config):
    indexer = ConfluenceIndexer(
        connection_config=connection_config,
        index_config=ConfluenceIndexerConfig(max_num_of_docs_from_each_space=251),
    )
    mock_client = mock.MagicMock()
    mock_client.get.side_effect = [
        {
            "results": [{"id": str(i)} for i in range(250)],
            "_links": {"next": "/wiki/api/v2/pages?cursor=next"},
        },
        {"results": [{"id": "250"}]},
    ]

    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client

        doc_ids = indexer._get_docs_ids_within_one_space(987)

    assert len(doc_ids) == 251
    assert doc_ids[-1] == {
        "space_id": 987,
        "doc_id": "250",
        "date_created": None,
        "date_modified": None,
        "version_number": None,
    }
    mock_client.get.assert_has_calls(
        [
            mock.call("api/v2/pages", params={"space-id": 987, "limit": 250}),
            mock.call("api/v2/pages?cursor=next", params=None),
        ],
        any_order=False,
    )


def test_downloader_uses_v2_page_api(tmp_path, connection_config):
    downloader = ConfluenceDownloader(
        connection_config=connection_config,
        download_config=ConfluenceDownloaderConfig(download_dir=tmp_path),
    )
    file_data = FileData(
        identifier="123",
        connector_type="confluence",
        source_identifiers=SourceIdentifiers(
            filename="123.html",
            fullpath="SPACE/123.html",
            rel_path="SPACE/123.html",
        ),
        metadata=FileDataSourceMetadata(url="https://dummy/pages/123"),
        additional_metadata={"space_id": 987},
    )
    mock_client = mock.MagicMock()
    mock_client.get.return_value = {
        "id": "123",
        "title": "Test Page",
        "createdAt": "2026-05-28T10:00:00Z",
        "version": {"createdAt": "2026-05-28T11:00:00Z", "number": 7},
        "body": {"view": {"value": "<p>Hello</p>"}},
    }

    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client
        response = downloader.run(file_data)

    # PLU-534: the downloader no longer fetches permissions (resolved at index time),
    # so the only client.get is the v2 page fetch.
    mock_client.get.assert_called_once_with(
        "api/v2/pages/123",
        params={"body-format": "view", "include-version": "true"},
    )
    assert response["path"] == tmp_path / "SPACE/123.html"
    assert response["file_data"].metadata.date_created == str(
        parser.parse("2026-05-28T10:00:00Z").timestamp()
    )
    assert response["file_data"].metadata.date_modified == str(
        parser.parse("2026-05-28T11:00:00Z").timestamp()
    )
    assert response["file_data"].metadata.version == "7"
    assert response["file_data"].display_name == "Test Page"
    assert (tmp_path / "SPACE/123.html").read_text(encoding="utf8")


def test_precheck_with_spaces_uses_v2_spaces(monkeypatch, connection_config):
    """Test that precheck uses the Confluence v2 spaces API for selected spaces."""
    spaces = ["A", "B", "C"]
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=100,
        max_num_of_docs_from_each_space=100,
        spaces=spaces,
    )
    indexer = ConfluenceIndexer(connection_config=connection_config, index_config=index_config)
    mock_client = mock.MagicMock()
    mock_client.get.side_effect = [
        {"results": [{"id": 1, "key": "A"}]},
        {"results": [{"id": 1, "key": "A"}]},
        {"results": [{"id": 2, "key": "B"}]},
        {"results": [{"id": 3, "key": "C"}]},
    ]
    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client

        result = indexer.precheck()
        calls = [
            mock.call("api/v2/spaces", params={"limit": 1}),
            *[mock.call("api/v2/spaces", params={"limit": 1, "keys": [space]}) for space in spaces],
        ]
        mock_client.get.assert_has_calls(calls, any_order=False)
        assert result is True


def test_precheck_without_spaces_uses_v2_spaces(monkeypatch, connection_config):
    """Test that precheck calls the Confluence v2 spaces API when spaces is not set."""
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=100,
        max_num_of_docs_from_each_space=100,
        spaces=None,
    )
    indexer = ConfluenceIndexer(connection_config=connection_config, index_config=index_config)
    mock_client = mock.MagicMock()
    mock_client.get.return_value = {"results": [{"id": 1, "key": "A"}]}
    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client

        result = indexer.precheck()
        mock_client.get.assert_called_once_with("api/v2/spaces", params={"limit": 1})
        assert result is True


def test_precheck_with_spaces_raises(monkeypatch, connection_config):
    """Test that precheck raises UserError if get_space fails."""
    spaces = ["A", "B"]
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=100,
        max_num_of_docs_from_each_space=100,
        spaces=spaces,
    )
    indexer = ConfluenceIndexer(connection_config=connection_config, index_config=index_config)
    mock_client = mock.MagicMock()
    mock_client.get.side_effect = [{"results": [{"id": 1, "key": "A"}]}, Exception("fail")]
    from unstructured_ingest.processes.connectors.confluence import UserError

    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client

        with pytest.raises(UserError):
            indexer.precheck()


def test_precheck_without_spaces_raises(monkeypatch, connection_config):
    """Test that precheck raises SourceConnectionError if listing spaces fails."""
    index_config = ConfluenceIndexerConfig(
        max_num_of_spaces=100,
        max_num_of_docs_from_each_space=100,
        spaces=None,
    )
    indexer = ConfluenceIndexer(connection_config=connection_config, index_config=index_config)
    mock_client = mock.MagicMock()
    mock_client.get.side_effect = Exception("fail")
    from unstructured_ingest.processes.connectors.confluence import UserError

    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client

        with pytest.raises(UserError):
            indexer.precheck()


def test_downloader_error_redacts_secret(tmp_path, connection_config):
    """A failing page fetch surfaces a SourceConnectionError with no raw exception text."""
    from unstructured_ingest.error import SourceConnectionError

    # A credential-bearing string of the kind an atlassian client error can embed;
    # it must never reach the raised SourceConnectionError message.
    secret = "password=secret&token=abc123XYZ"

    downloader = ConfluenceDownloader(
        connection_config=connection_config,
        download_config=ConfluenceDownloaderConfig(download_dir=tmp_path),
    )
    file_data = FileData(
        identifier="123",
        connector_type="confluence",
        source_identifiers=SourceIdentifiers(
            filename="123.html",
            fullpath="SPACE/123.html",
            rel_path="SPACE/123.html",
        ),
        metadata=FileDataSourceMetadata(url="https://dummy/pages/123"),
        additional_metadata={"space_id": 987},
    )
    mock_client = mock.MagicMock()
    mock_client.get.side_effect = Exception(secret)

    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client
        with pytest.raises(SourceConnectionError) as excinfo:
            downloader.run(file_data)

    message = str(excinfo.value)
    assert secret not in message
    assert "abc123XYZ" not in message
    # The page identifier and the sanitized summary survive for troubleshooting.
    assert "123" in message
    assert "Exception" in message


# --- PLU-534: ACL digest (permissions_version) ---------------------------------

CONFLUENCE_MODULE = "unstructured_ingest.processes.connectors.confluence"


def test_get_permissions_data_none_when_space_fetch_unavailable(connection_config):
    # A failed space-permission fetch is "unavailable", not "empty": return None so
    # the caller leaves the ACL fields unset rather than fabricating an empty ACL.
    mock_client = mock.MagicMock()
    mock_client.get.side_effect = Exception("boom")

    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client
        result = get_permissions_data(
            connection_config=connection_config,
            doc_id="123",
            space_id=987,
            cache=OrderedDict(),
            max_num_metadata_permissions=250,
        )

    assert result is None


def test_get_permissions_data_returns_normalized_permissions(connection_config):
    space_permissions = [
        {
            "operation": {"key": "read", "targetType": "space"},
            "principal": {"id": "u-space", "type": "user"},
        },
    ]
    mock_client = mock.MagicMock()
    mock_client.get.return_value = {"results": space_permissions, "_links": {}}
    mock_client.get_all_restrictions_for_content.return_value = {
        "read": {
            "restrictions": {
                "user": {"results": [{"accountId": "u1"}]},
                "group": {"results": []},
            }
        },
        "update": {"restrictions": {"user": {"results": []}, "group": {"results": []}}},
    }

    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client
        result = get_permissions_data(
            connection_config=connection_config,
            doc_id="123",
            space_id=987,
            cache=OrderedDict(),
            max_num_metadata_permissions=250,
        )

    assert result is not None
    assert [next(iter(entry)) for entry in result] == ["read", "update", "delete"]
    read = next(entry["read"] for entry in result if "read" in entry)
    # page-level read restriction is captured
    assert "u1" in read["users"]


def test_get_permissions_data_is_order_independent(connection_config):
    # Determinism: the same permissions in a different API order must produce the
    # same normalized result (and therefore the same digest).
    def run_with_user_order(user_results):
        mock_client = mock.MagicMock()
        mock_client.get.return_value = {"results": [], "_links": {}}
        mock_client.get_all_restrictions_for_content.return_value = {
            "read": {
                "restrictions": {
                    "user": {"results": user_results},
                    "group": {"results": []},
                }
            },
        }
        with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
            type(connection_config).get_client.return_value.__enter__.return_value = mock_client
            return get_permissions_data(
                connection_config=connection_config,
                doc_id="123",
                space_id=987,
                cache=OrderedDict(),
                max_num_metadata_permissions=250,
            )

    a = run_with_user_order([{"accountId": "u1"}, {"accountId": "u2"}])
    b = run_with_user_order([{"accountId": "u2"}, {"accountId": "u1"}])
    assert a == b
    assert compute_permissions_version(a) == compute_permissions_version(b)


def _indexer_with_docs(connection_config, docs):
    indexer = ConfluenceIndexer(
        connection_config=connection_config,
        index_config=ConfluenceIndexerConfig(spaces=["ENG"]),
    )
    return indexer, docs


_DOC = {
    "space_id": 987,
    "doc_id": "123",
    "date_created": None,
    "date_modified": None,
    "version_number": 7,
}


def test_indexer_emits_permissions_version(connection_config):
    indexer, docs = _indexer_with_docs(connection_config, [_DOC])
    permissions_data = [
        {"read": {"users": ["u1"], "groups": ["g1"]}},
        {"update": {"users": [], "groups": []}},
        {"delete": {"users": [], "groups": []}},
    ]
    with (
        mock.patch.object(indexer, "_get_space_ids_and_keys", return_value=[("ENG", 987)]),
        mock.patch.object(indexer, "_get_docs_ids_within_one_space", return_value=docs),
        mock.patch(f"{CONFLUENCE_MODULE}.get_permissions_data", return_value=permissions_data),
    ):
        results = list(indexer.run())

    assert len(results) == 1
    fd = results[0]
    assert fd.metadata.version == "7"
    assert fd.metadata.permissions_data == permissions_data
    assert fd.metadata.permissions_version == compute_permissions_version(permissions_data)


def test_indexer_permissions_unavailable_leaves_version_unset(connection_config):
    indexer, docs = _indexer_with_docs(connection_config, [_DOC])
    with (
        mock.patch.object(indexer, "_get_space_ids_and_keys", return_value=[("ENG", 987)]),
        mock.patch.object(indexer, "_get_docs_ids_within_one_space", return_value=docs),
        mock.patch(f"{CONFLUENCE_MODULE}.get_permissions_data", return_value=None),
    ):
        results = list(indexer.run())

    assert len(results) == 1
    fd = results[0]
    # content version still emitted; ACL fields left unset so the compare is skipped
    assert fd.metadata.version == "7"
    assert fd.metadata.permissions_data is None
    assert fd.metadata.permissions_version is None


# --- PLU-625: access-class principals must not crash the permission parser ---------


def _run_get_permissions_data(connection_config, space_permissions, doc_restrictions):
    mock_client = mock.MagicMock()
    mock_client.get.return_value = {"results": space_permissions, "_links": {}}
    mock_client.get_all_restrictions_for_content.return_value = doc_restrictions
    with mock.patch.object(type(connection_config), "get_client", mock.MagicMock()):
        type(connection_config).get_client.return_value.__enter__.return_value = mock_client
        return get_permissions_data(
            connection_config=connection_config,
            doc_id="123",
            space_id=987,
            cache=OrderedDict(),
            max_num_metadata_permissions=250,
        )


_NO_PAGE_RESTRICTION = {
    "read": {"restrictions": {"user": {"results": []}, "group": {"results": []}}},
    "update": {"restrictions": {"user": {"results": []}, "group": {"results": []}}},
}


def test_parse_handles_access_class_space_permissions(connection_config):
    # ALL_PRODUCT_ADMINS is an access-class principal present on essentially every space.
    # Before PLU-625 this raised KeyError (access-classs bucket) -> swallowed -> None.
    space_permissions = [
        {
            "operation": {"key": "administer", "targetType": "space"},
            "principal": {"id": "ALL_PRODUCT_ADMINS", "type": "access-class"},
        },
        {
            "operation": {"key": "delete", "targetType": "page"},
            "principal": {"id": "ALL_PRODUCT_ADMINS", "type": "access-class"},
        },
    ]
    result = _run_get_permissions_data(connection_config, space_permissions, _NO_PAGE_RESTRICTION)

    assert result is not None  # would be None (KeyError swallowed) before the fix
    read = next(entry["read"] for entry in result if "read" in entry)
    # access-class captured in its own bucket, alongside users/groups
    assert set(read.keys()) == {"users", "groups", "access_classes"}
    assert "ALL_PRODUCT_ADMINS" in read["access_classes"]
    delete = next(entry["delete"] for entry in result if "delete" in entry)
    assert "ALL_PRODUCT_ADMINS" in delete["access_classes"]


def test_page_restriction_survives_access_class_space_permissions(connection_config):
    # The regression: a page IS restricted to a named user, but the space also carries
    # an access-class delete/page perm whose branch condition evaluated the missing
    # bucket -> KeyError -> the whole parse threw and discarded the page restriction too.
    space_permissions = [
        {
            "operation": {"key": "delete", "targetType": "page"},
            "principal": {"id": "ALL_PRODUCT_ADMINS", "type": "access-class"},
        },
    ]
    doc_restrictions = {
        "read": {
            "restrictions": {"user": {"results": [{"accountId": "u1"}]}, "group": {"results": []}}
        },
        "update": {"restrictions": {"user": {"results": []}, "group": {"results": []}}},
    }
    result = _run_get_permissions_data(connection_config, space_permissions, doc_restrictions)

    assert result is not None  # previously None
    read = next(entry["read"] for entry in result if "read" in entry)
    # the explicit page restriction is preserved (it was being discarded by the crash)
    assert "u1" in read["users"]


def test_parse_skips_unknown_principal_type(connection_config):
    # A principal type we don't model should be skipped, not crash.
    space_permissions = [
        {
            "operation": {"key": "administer", "targetType": "space"},
            "principal": {"id": "weird", "type": "some-future-type"},
        },
    ]
    result = _run_get_permissions_data(connection_config, space_permissions, _NO_PAGE_RESTRICTION)
    assert result is not None
