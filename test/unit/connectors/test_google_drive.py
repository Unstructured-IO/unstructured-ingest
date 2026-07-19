import json
from unittest.mock import MagicMock

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import SourceConnectionError, ValueError
from unstructured_ingest.processes.connectors.google_drive import (
    GOOGLE_DRIVE_SKIP_MIME_TYPES,
    GOOGLE_EXPORT_MIME_MAP,
    GoogleDriveAccessConfig,
    GoogleDriveConnectionConfig,
    GoogleDriveDownloader,
    GoogleDriveDownloaderConfig,
    GoogleDriveIndexer,
    GoogleDriveIndexerConfig,
    _get_extension,
    _should_skip_file,
)


def _make_file_data(mime_type: str, filename: str = "native-file") -> FileData:
    return FileData(
        connector_type="google_drive",
        identifier="file-id",
        source_identifiers=SourceIdentifiers(filename=filename, fullpath=filename),
        additional_metadata={"mimeType": mime_type, "size": "0"},
    )


class _FakeGoogleDriveRequest:
    def __init__(self, response: dict):
        self.response = response

    def execute(self) -> dict:
        return self.response


class _FakeGoogleDriveFilesClient:
    def __init__(self, responses: list[dict]):
        self.responses = responses
        self.list_calls = []

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        return _FakeGoogleDriveRequest(self.responses.pop(0))


class TestGoogleDriveAccessConfig:
    """Tests for GoogleDriveAccessConfig authentication validation."""

    def test_oauth_token_only(self):
        """OAuth token alone should be valid."""
        config = GoogleDriveAccessConfig(oauth_token="ya29.a0AfH6SMBxxxxxxxx")
        assert config.oauth_token == "ya29.a0AfH6SMBxxxxxxxx"

    def test_service_account_key_only(self):
        """Service account key alone should be valid."""
        config = GoogleDriveAccessConfig(
            service_account_key={"type": "service_account", "project_id": "test"}
        )
        assert config.service_account_key == {"type": "service_account", "project_id": "test"}
        assert config.oauth_token is None

    def test_service_account_key_as_json_string(self):
        """Service account key as JSON string should be valid."""
        key_dict = {"type": "service_account", "project_id": "test"}
        config = GoogleDriveAccessConfig(service_account_key=json.dumps(key_dict))
        assert config.service_account_key == key_dict

    def test_no_auth_raises_error(self):
        """No authentication provided should raise ValueError."""
        with pytest.raises(ValueError, match="must be set"):
            GoogleDriveAccessConfig()

    def test_both_oauth_and_service_account_raises_error(self):
        """Both auth methods provided should raise ValueError."""
        with pytest.raises(ValueError, match="cannot use both"):
            GoogleDriveAccessConfig(
                service_account_key={"type": "service_account"},
                oauth_token="ya29.a0AfH6SMBxxxxxxxx",
            )

    def test_both_oauth_and_service_account_path_raises_error(self, tmp_path):
        """OAuth token + service account path should raise ValueError."""
        key_file = tmp_path / "credentials.json"
        key_file.write_text('{"type": "service_account", "project_id": "test"}')

        with pytest.raises(ValueError, match="cannot use both"):
            GoogleDriveAccessConfig(
                service_account_key_path=key_file,
                oauth_token="ya29.a0AfH6SMBxxxxxxxx",
            )

    def test_service_account_key_path_only(self, tmp_path):
        """Service account key path alone should be valid."""
        key_file = tmp_path / "credentials.json"
        key_file.write_text('{"type": "service_account", "project_id": "test"}')

        config = GoogleDriveAccessConfig(service_account_key_path=key_file)
        assert config.service_account_key_path == key_file
        assert config.oauth_token is None

    def test_get_service_account_key_from_path(self, tmp_path):
        """get_service_account_key should load from file path."""
        key_data = {"type": "service_account", "project_id": "test", "private_key": "xxx"}
        key_file = tmp_path / "credentials.json"
        key_file.write_text(json.dumps(key_data))

        config = GoogleDriveAccessConfig(service_account_key_path=key_file)
        result = config.get_service_account_key()
        assert result == key_data

    def test_get_service_account_key_from_dict(self):
        """get_service_account_key should return the dict directly."""
        key_data = {"type": "service_account", "project_id": "test"}
        config = GoogleDriveAccessConfig(service_account_key=key_data)
        result = config.get_service_account_key()
        assert result == key_data

    def test_service_account_key_and_path_same_value(self, tmp_path):
        """If both key and path are provided with same value, should succeed."""
        key_data = {"type": "service_account", "project_id": "test"}
        key_file = tmp_path / "credentials.json"
        key_file.write_text('{"type": "service_account", "project_id": "test"}')

        config = GoogleDriveAccessConfig(
            service_account_key=key_data,
            service_account_key_path=key_file,
        )
        result = config.get_service_account_key()
        assert result == key_data

    def test_service_account_key_and_path_different_value_raises(self, tmp_path):
        """If both key and path are provided with different values, should raise."""
        key_data = {"type": "service_account", "project_id": "test1"}
        key_file = tmp_path / "credentials.json"
        key_file.write_text('{"type": "service_account", "project_id": "test2"}')

        config = GoogleDriveAccessConfig(
            service_account_key=key_data,
            service_account_key_path=key_file,
        )
        with pytest.raises(ValueError, match="both provided and have different values"):
            config.get_service_account_key()


class TestGoogleDriveNativeExports:
    @pytest.mark.parametrize(
        ("source_mime_type", "expected_extension"),
        [
            ("application/vnd.google-apps.document", ".docx"),
            ("application/vnd.google-apps.spreadsheet", ".xlsx"),
            ("application/vnd.google-apps.presentation", ".pptx"),
            ("application/vnd.google-apps.drawing", ".png"),
        ],
    )
    def test_get_extension_uses_source_mime_type(self, source_mime_type, expected_extension):
        file_data = _make_file_data(source_mime_type)

        assert _get_extension(file_data) == expected_extension

    @pytest.mark.parametrize(
        ("source_mime_type", "expected_extension"),
        [
            ("application/vnd.google-apps.document", ".docx"),
            ("application/vnd.google-apps.spreadsheet", ".xlsx"),
            ("application/vnd.google-apps.presentation", ".pptx"),
            ("application/vnd.google-apps.drawing", ".png"),
        ],
    )
    def test_downloader_exports_supported_google_native_files(
        self, tmp_path, source_mime_type, expected_extension
    ):
        file_data = _make_file_data(source_mime_type)
        downloader = GoogleDriveDownloader(
            connection_config=MagicMock(),
            download_config=GoogleDriveDownloaderConfig(download_dir=tmp_path),
        )
        export_calls = []
        direct_download = MagicMock()

        def export_file(file_id, download_path, mime_type, file_size):
            export_calls.append(
                {
                    "file_id": file_id,
                    "download_path": download_path,
                    "mime_type": mime_type,
                    "file_size": file_size,
                }
            )
            download_path.write_bytes(b"exported")

        downloader._export_gdrive_native_file = export_file
        downloader._direct_download_file = direct_download

        download_path = downloader._download_file(file_data)

        assert download_path.suffix == expected_extension
        assert download_path.exists()
        assert export_calls == [
            {
                "file_id": "file-id",
                "download_path": download_path,
                "mime_type": GOOGLE_EXPORT_MIME_MAP[source_mime_type],
                "file_size": 0,
            }
        ]
        direct_download.assert_not_called()
        assert (
            file_data.additional_metadata["export_mime_type"]
            == GOOGLE_EXPORT_MIME_MAP[source_mime_type]
        )
        assert file_data.additional_metadata["export_extension"] == expected_extension
        assert file_data.additional_metadata["download_method"] == "google_workspace_export"

    @pytest.mark.parametrize("mime_type", sorted(GOOGLE_DRIVE_SKIP_MIME_TYPES))
    def test_downloader_skips_non_downloadable_native_files(self, tmp_path, mime_type):
        file_data = _make_file_data(mime_type)
        downloader = GoogleDriveDownloader(
            connection_config=MagicMock(),
            download_config=GoogleDriveDownloaderConfig(download_dir=tmp_path),
        )
        downloader._export_gdrive_native_file = MagicMock()
        downloader._direct_download_file = MagicMock()

        result = downloader._download_file(file_data)

        assert result is None
        downloader._export_gdrive_native_file.assert_not_called()
        downloader._direct_download_file.assert_not_called()

    def test_downloader_rejects_unknown_native_mime_types(self, tmp_path):
        """Native types not in skip list and not exportable should still raise."""
        file_data = _make_file_data("application/vnd.google-apps.unknown_future_type")
        downloader = GoogleDriveDownloader(
            connection_config=MagicMock(),
            download_config=GoogleDriveDownloaderConfig(download_dir=tmp_path),
        )
        downloader._export_gdrive_native_file = MagicMock()
        downloader._direct_download_file = MagicMock()

        with pytest.raises(
            SourceConnectionError, match="Unsupported Google Drive native MIME type"
        ):
            downloader._download_file(file_data)

        downloader._export_gdrive_native_file.assert_not_called()
        downloader._direct_download_file.assert_not_called()


class TestGoogleDriveSkipFiles:
    """Tests for _should_skip_file and downloader skip behavior for non-downloadable files."""

    @pytest.mark.parametrize("mime_type", sorted(GOOGLE_DRIVE_SKIP_MIME_TYPES))
    def test_should_skip_file_returns_true_for_skip_mimes(self, mime_type):
        assert _should_skip_file({"mimeType": mime_type}) is True

    def test_should_skip_file_returns_true_for_inode_x_empty(self):
        assert _should_skip_file({"mimeType": "inode/x-empty", "size": "0"}) is True

    def test_should_skip_file_returns_true_for_zero_size_no_mime(self):
        assert _should_skip_file({"mimeType": "", "size": "0"}) is True

    def test_should_skip_file_returns_false_for_normal_file(self):
        assert _should_skip_file({"mimeType": "application/pdf", "size": "1024"}) is False

    def test_should_skip_file_returns_false_for_exportable_native(self):
        assert (
            _should_skip_file({"mimeType": "application/vnd.google-apps.document", "size": "0"})
            is False
        )

    def test_downloader_skips_inode_x_empty(self, tmp_path):
        file_data = _make_file_data("inode/x-empty")
        downloader = GoogleDriveDownloader(
            connection_config=MagicMock(),
            download_config=GoogleDriveDownloaderConfig(download_dir=tmp_path),
        )
        downloader._export_gdrive_native_file = MagicMock()
        downloader._direct_download_file = MagicMock()

        result = downloader._download_file(file_data)

        assert result is None
        downloader._export_gdrive_native_file.assert_not_called()
        downloader._direct_download_file.assert_not_called()

    def test_downloader_run_returns_empty_list_for_skipped_file(self, tmp_path):
        file_data = _make_file_data("application/vnd.google-apps.shortcut")
        downloader = GoogleDriveDownloader(
            connection_config=MagicMock(),
            download_config=GoogleDriveDownloaderConfig(download_dir=tmp_path),
        )
        downloader._export_gdrive_native_file = MagicMock()
        downloader._direct_download_file = MagicMock()

        result = downloader.run(file_data)

        assert result == []

    def test_indexer_filters_skip_mime_types_from_paginated_results(self):
        files_client = MagicMock()
        files_client.list.return_value.execute.return_value = {
            "files": [
                {"id": "pdf-1", "name": "doc.pdf", "mimeType": "application/pdf"},
                {
                    "id": "shortcut-1",
                    "name": "alias",
                    "mimeType": "application/vnd.google-apps.shortcut",
                },
                {"id": "form-1", "name": "survey", "mimeType": "application/vnd.google-apps.form"},
                {"id": "pdf-2", "name": "doc2.pdf", "mimeType": "application/pdf"},
                {"id": "empty-1", "name": "empty", "mimeType": "inode/x-empty", "size": "0"},
            ]
        }
        indexer = GoogleDriveIndexer(
            connection_config=MagicMock(),
            index_config=GoogleDriveIndexerConfig(),
        )

        results = indexer.get_paginated_results(
            files_client=files_client,
            object_id="folder-id",
        )

        result_ids = [r["id"] for r in results]
        assert result_ids == ["pdf-1", "pdf-2"]

    def test_count_files_recursively_excludes_skip_mimes(self):
        files_client = _FakeGoogleDriveFilesClient(
            responses=[
                {
                    "files": [
                        {"id": "pdf-1", "mimeType": "application/pdf", "fileExtension": "pdf"},
                        {
                            "id": "shortcut",
                            "mimeType": "application/vnd.google-apps.shortcut",
                        },
                        {"id": "form", "mimeType": "application/vnd.google-apps.form"},
                        {"id": "empty", "mimeType": "inode/x-empty", "size": "0"},
                        {
                            "id": "doc",
                            "mimeType": "application/vnd.google-apps.document",
                        },
                    ]
                }
            ]
        )

        count = GoogleDriveIndexer.count_files_recursively(
            files_client=files_client,
            folder_id="folder-id",
        )

        # pdf-1 + doc = 2; shortcut, form, empty are all skipped
        assert count == 2


class TestGoogleDriveExcludesTrashed:
    """The indexer must filter out trashed Drive items by passing `trashed = false`
    in its `files.list` queries. Without it, Drive returns trashed items in
    shared-drive corpora and the bug fires."""

    def test_get_paginated_results_query_excludes_trashed(self):
        files_client = MagicMock()
        files_client.list.return_value.execute.return_value = {"files": []}
        indexer = GoogleDriveIndexer(
            connection_config=MagicMock(),
            index_config=GoogleDriveIndexerConfig(),
        )

        indexer.get_paginated_results(files_client=files_client, object_id="folder-id")

        query = files_client.list.call_args.kwargs["q"]
        assert "trashed = false" in query

    def test_get_paginated_results_query_excludes_trashed_with_extensions(self):
        files_client = MagicMock()
        files_client.list.return_value.execute.return_value = {"files": []}
        indexer = GoogleDriveIndexer(
            connection_config=MagicMock(),
            index_config=GoogleDriveIndexerConfig(),
        )

        indexer.get_paginated_results(
            files_client=files_client,
            object_id="folder-id",
            extensions=["pdf"],
        )

        query = files_client.list.call_args.kwargs["q"]
        assert "trashed = false" in query

    def test_count_files_recursively_query_excludes_trashed(self):
        files_client = _FakeGoogleDriveFilesClient(responses=[{"files": []}])

        GoogleDriveIndexer.count_files_recursively(
            files_client=files_client,
            folder_id="folder-id",
        )

        assert len(files_client.list_calls) == 1
        assert "trashed = false" in files_client.list_calls[0]["q"]

    def test_precheck_non_recursive_empty_folder_query_excludes_trashed(self, monkeypatch):
        connection_config = GoogleDriveConnectionConfig(
            drive_id="drive-id",
            access_config=GoogleDriveAccessConfig(oauth_token="t"),
        )
        indexer = GoogleDriveIndexer(
            connection_config=connection_config,
            index_config=GoogleDriveIndexerConfig(recursive=False),
        )

        files_client = MagicMock()
        files_client.list.return_value.execute.return_value = {"files": []}

        class _FakeClientCtx:
            def __enter__(self_inner):
                return files_client

            def __exit__(self_inner, *args):
                return False

        monkeypatch.setattr(
            GoogleDriveConnectionConfig, "get_client", lambda self: _FakeClientCtx()
        )
        monkeypatch.setattr(
            GoogleDriveIndexer, "verify_drive_api_enabled", staticmethod(lambda client: None)
        )
        monkeypatch.setattr(
            indexer,
            "get_root_info",
            lambda files_client, object_id: {
                "id": object_id,
                "name": "root",
                "mimeType": "application/vnd.google-apps.folder",
            },
        )

        indexer.precheck()

        # First list call after the precheck path enters the non-recursive branch.
        empty_folder_call = files_client.list.call_args
        assert "trashed = false" in empty_folder_call.kwargs["q"]


class TestGoogleDriveExtensionFiltering:
    def test_get_paginated_results_includes_native_mimes_for_export_extensions(self):
        files_client = MagicMock()
        files_client.list.return_value.execute.return_value = {"files": []}
        indexer = GoogleDriveIndexer(
            connection_config=MagicMock(),
            index_config=GoogleDriveIndexerConfig(),
        )

        indexer.get_paginated_results(
            files_client=files_client,
            object_id="folder-id",
            extensions=["docx", ".xlsx", "pptx", "png"],
        )

        query = files_client.list.call_args.kwargs["q"]
        assert "fileExtension = 'docx'" in query
        assert "fileExtension = 'xlsx'" in query
        assert "fileExtension = 'pptx'" in query
        assert "fileExtension = 'png'" in query
        assert "mimeType = 'application/vnd.google-apps.document'" in query
        assert "mimeType = 'application/vnd.google-apps.spreadsheet'" in query
        assert "mimeType = 'application/vnd.google-apps.presentation'" in query
        assert "mimeType = 'application/vnd.google-apps.drawing'" in query
        assert "mimeType = 'application/vnd.google-apps.folder'" in query

    def test_count_files_recursively_matches_native_mimes_for_export_extensions(self):
        files_client = _FakeGoogleDriveFilesClient(
            responses=[
                {
                    "files": [
                        {
                            "id": "doc",
                            "mimeType": "application/vnd.google-apps.document",
                        },
                        {
                            "id": "drawing",
                            "mimeType": "application/vnd.google-apps.drawing",
                        },
                        {
                            "id": "form",
                            "mimeType": "application/vnd.google-apps.form",
                        },
                        {
                            "id": "binary-docx",
                            "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",  # noqa: E501
                            "fileExtension": "docx",
                        },
                        {
                            "id": "pdf",
                            "mimeType": "application/pdf",
                            "fileExtension": "pdf",
                        },
                    ]
                }
            ]
        )

        count = GoogleDriveIndexer.count_files_recursively(
            files_client=files_client,
            folder_id="folder-id",
            extensions=["docx", "png"],
        )

        assert count == 3


class TestGoogleDriveRecordLocator:
    """The indexer must stamp the source drive id onto every file's
    record_locator so downstream consumers can resolve which drive a file
    came from."""

    @staticmethod
    def _indexer() -> GoogleDriveIndexer:
        return GoogleDriveIndexer(
            connection_config=MagicMock(),
            index_config=GoogleDriveIndexerConfig(),
        )

    def test_get_files_sets_drive_id_for_single_file(self):
        indexer = self._indexer()
        files_client = MagicMock()
        files_client.get.return_value.execute.return_value = {
            "id": "file-1",
            "name": "doc.pdf",
            "mimeType": "application/pdf",
            "createdTime": "2024-01-01T00:00:00.000Z",
            "modifiedTime": "2024-01-02T00:00:00.000Z",
        }

        data = indexer.get_files(files_client=files_client, object_id="drive-123")

        assert len(data) == 1
        assert data[0].metadata.record_locator["drive_id"] == "drive-123"
        assert data[0].metadata.record_locator["file_id"] == "file-1"

    def test_get_files_sets_drive_id_for_every_file_in_folder(self, monkeypatch):
        indexer = self._indexer()

        monkeypatch.setattr(
            indexer,
            "get_root_info",
            lambda files_client, object_id: {
                "id": object_id,
                "name": "root",
                "mimeType": "application/vnd.google-apps.folder",
            },
        )
        monkeypatch.setattr(
            indexer,
            "get_paginated_results",
            lambda **kwargs: [
                {
                    "id": "file-1",
                    "name": "a.pdf",
                    "mimeType": "application/pdf",
                    "createdTime": "2024-01-01T00:00:00.000Z",
                    "modifiedTime": "2024-01-02T00:00:00.000Z",
                },
                {
                    "id": "file-2",
                    "name": "b.pdf",
                    "mimeType": "application/pdf",
                    "createdTime": "2024-01-01T00:00:00.000Z",
                    "modifiedTime": "2024-01-02T00:00:00.000Z",
                },
            ],
        )

        data = indexer.get_files(files_client=MagicMock(), object_id="drive-xyz")

        assert len(data) == 2
        assert all(d.metadata.record_locator["drive_id"] == "drive-xyz" for d in data)
