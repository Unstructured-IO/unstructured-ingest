import os
import uuid

import pytest
from googleapiclient.errors import HttpError

from test.integration.connectors.utils.constants import (
    SOURCE_TAG,
)
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
    source_filedata_display_name_set_check,
)
from test.integration.utils import requires_env
from unstructured_ingest.error import (
    SourceConnectionError,
)
from unstructured_ingest.processes.connectors.google_drive import (
    CONNECTOR_TYPE,
    GoogleDriveAccessConfig,
    GoogleDriveConnectionConfig,
    GoogleDriveDownloader,
    GoogleDriveDownloaderConfig,
    GoogleDriveIndexer,
    GoogleDriveIndexerConfig,
)


@pytest.fixture
def google_drive_connection_config():
    """
    Build a valid GoogleDriveConnectionConfig using the environment variables.
    Expects:
      - GOOGLE_DRIVE_ID
      - GOOGLE_DRIVE_SERVICE_KEY
    """
    drive_id = os.getenv("GOOGLE_DRIVE_ID")
    service_key = os.getenv("GOOGLE_DRIVE_SERVICE_KEY")
    if not drive_id or not service_key:
        pytest.skip("Google Drive credentials not provided in environment variables.")

    access_config = GoogleDriveAccessConfig(service_account_key=service_key)
    return GoogleDriveConnectionConfig(drive_id=drive_id, access_config=access_config)


@pytest.fixture
def google_drive_empty_folder(google_drive_connection_config):
    """
    Creates an empty folder on Google Drive for testing the "empty folder" case.
    The folder is deleted after the test.
    """
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    access_config = google_drive_connection_config.access_config.get_secret_value()
    creds = service_account.Credentials.from_service_account_info(access_config.service_account_key)
    service = build("drive", "v3", credentials=creds)

    # Create an empty folder.
    file_metadata = {
        "name": f"utic-empty-folder-{uuid.uuid4()}",
        "mimeType": "application/vnd.google-apps.folder",
    }
    folder = service.files().create(body=file_metadata, fields="id, name").execute()
    folder_id = folder.get("id")
    try:
        yield folder_id
    finally:
        service.files().delete(fileId=folder_id).execute()


@pytest.fixture
def google_drive_folder_with_trashed_file(google_drive_connection_config):
    """
    Creates a temporary folder with two files; trashes one. Yields a dict with
    the folder id, the trashed file id, and the live file id. Cleans up on exit.
    """
    import io

    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload

    access_config = google_drive_connection_config.access_config.get_secret_value()
    creds = service_account.Credentials.from_service_account_info(access_config.service_account_key)
    service = build("drive", "v3", credentials=creds)

    # Nest the fixture folder under the env-provided drive id so it inherits
    # that parent's storage quota. Service accounts have no My Drive quota of
    # their own, so a parentless folder.create raises storageQuotaExceeded.
    parent_id = google_drive_connection_config.drive_id
    folder = (
        service.files()
        .create(
            body={
                "name": f"utic-trashed-fixture-{uuid.uuid4()}",
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_id],
            },
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    folder_id = folder["id"]

    def _upload(name: str) -> str:
        media = MediaIoBaseUpload(io.BytesIO(b"hello"), mimetype="text/plain")
        created = (
            service.files()
            .create(
                body={"name": name, "parents": [folder_id], "mimeType": "text/plain"},
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
        return created["id"]

    live_id = _upload("live.txt")
    trashed_id = _upload("trashed.txt")
    service.files().update(
        fileId=trashed_id, body={"trashed": True}, supportsAllDrives=True
    ).execute()

    try:
        yield {"folder_id": folder_id, "live_id": live_id, "trashed_id": trashed_id}
    finally:
        # Permanently delete fixture folder (and its children, trashed or not).
        service.files().delete(fileId=folder_id, supportsAllDrives=True).execute()


@pytest.mark.tags("google-drive", "integration")
@requires_env("GOOGLE_DRIVE_ID", "GOOGLE_DRIVE_SERVICE_KEY")
def test_google_drive_indexer_excludes_trashed_files(
    google_drive_connection_config, google_drive_folder_with_trashed_file
):
    """
    Regression test for the connector silently ingesting Drive items the user
    has moved to trash. Without `trashed = false` on `files.list`, the indexer
    returns both files in the fixture folder; with it, only the live one.
    """
    fixture = google_drive_folder_with_trashed_file
    connection_config = GoogleDriveConnectionConfig(
        drive_id=fixture["folder_id"],
        access_config=google_drive_connection_config.access_config,
    )
    indexer = GoogleDriveIndexer(
        connection_config=connection_config,
        index_config=GoogleDriveIndexerConfig(recursive=True),
    )

    with connection_config.get_client() as client:
        results = indexer.get_paginated_results(
            files_client=client,
            object_id=fixture["folder_id"],
            recursive=True,
        )

    returned_ids = {r["id"] for r in results}
    assert fixture["live_id"] in returned_ids
    assert fixture["trashed_id"] not in returned_ids


@requires_env("GOOGLE_DRIVE_SERVICE_KEY")
@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE)
def test_google_drive_source(temp_dir):
    # Retrieve environment variables
    service_account_key = os.environ["GOOGLE_DRIVE_SERVICE_KEY"]

    # Create connection and indexer configurations
    access_config = GoogleDriveAccessConfig(service_account_key=service_account_key)
    connection_config = GoogleDriveConnectionConfig(
        drive_id="1XidSOO76VpZ4m0i3gJN2m1X0Obol3UAi",
        access_config=access_config,
    )
    index_config = GoogleDriveIndexerConfig(recursive=True)

    download_config = GoogleDriveDownloaderConfig(download_dir=temp_dir)

    # Instantiate indexer and downloader
    indexer = GoogleDriveIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = GoogleDriveDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    # Run the source connector validation
    source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="google_drive_source",
            expected_num_files=1,
            validate_downloaded_files=True,
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
        ),
    )


# Precheck fails when the drive ID has an appended parameter (simulate copy-paste error)
@pytest.mark.tags("google-drive", "precheck")
@requires_env("GOOGLE_DRIVE_ID", "GOOGLE_DRIVE_SERVICE_KEY")
def test_google_drive_precheck_invalid_parameter(google_drive_connection_config):
    # Append a query parameter as often happens when copying from a URL.
    invalid_drive_id = google_drive_connection_config.drive_id + "?usp=sharing"
    connection_config = GoogleDriveConnectionConfig(
        drive_id=invalid_drive_id,
        access_config=google_drive_connection_config.access_config,
    )
    index_config = GoogleDriveIndexerConfig(recursive=True)
    indexer = GoogleDriveIndexer(connection_config=connection_config, index_config=index_config)
    with pytest.raises(SourceConnectionError) as excinfo:
        indexer.precheck()
    assert "invalid" in str(excinfo.value).lower() or "not found" in str(excinfo.value).lower()


# Precheck fails due to lack of permission (simulate via monkeypatching).
@pytest.mark.tags("google-drive", "precheck")
@requires_env("GOOGLE_DRIVE_ID", "GOOGLE_DRIVE_SERVICE_KEY")
def test_google_drive_precheck_no_permission(google_drive_connection_config, monkeypatch):
    index_config = GoogleDriveIndexerConfig(recursive=True)
    indexer = GoogleDriveIndexer(
        connection_config=google_drive_connection_config,
        index_config=index_config,
    )

    # Monkeypatch get_root_info to always raise an HTTP 403 error.
    def fake_get_root_info(files_client, object_id):
        raise HttpError(
            resp=type("Response", (), {"status": 403, "reason": "Forbidden"})(),
            content=b"Forbidden",
        )

    monkeypatch.setattr(indexer, "get_root_info", fake_get_root_info)
    with pytest.raises(SourceConnectionError) as excinfo:
        indexer.precheck()
    assert "forbidden" in str(excinfo.value).lower() or "permission" in str(excinfo.value).lower()


# Precheck fails when the folder is empty.
# @pytest.mark.tags("google-drive", "precheck")
# @requires_env("GOOGLE_DRIVE_ID", "GOOGLE_DRIVE_SERVICE_KEY")
# def test_google_drive_precheck_empty_folder(
#     google_drive_connection_config, google_drive_empty_folder
# ):
#     # Use the empty folder's ID as the target.
#     connection_config = GoogleDriveConnectionConfig(
#         drive_id=google_drive_empty_folder,
#         access_config=google_drive_connection_config.access_config,
#     )

#     index_config = GoogleDriveIndexerConfig(recursive=True)
#     indexer = GoogleDriveIndexer(connection_config=connection_config, index_config=index_config)
#     with pytest.raises(SourceConnectionError) as excinfo:
#         indexer.precheck()
#     assert "empty folder" in str(excinfo.value).lower()


@pytest.mark.tags("google-drive", "count", "integration")
@requires_env("GOOGLE_DRIVE_ID", "GOOGLE_DRIVE_SERVICE_KEY")
def test_google_drive_count_files(google_drive_connection_config):
    """
    This test verifies that the count_files_recursively method returns the expected count of files.
    According to the test credentials, there are 3 files in the root directory and 1 nested file,
    so the total count should be 4.
    """
    # I assumed that we're applying the same extension filter as with other tests
    # However there's 6 files in total in the test dir
    extensions_filter = ["pdf", "docx"]
    with google_drive_connection_config.get_client() as client:
        count = GoogleDriveIndexer.count_files_recursively(
            client, google_drive_connection_config.drive_id, extensions_filter
        )
    assert count == 4, f"Expected file count of 4, but got {count}"


# Precheck fails with a completely invalid drive ID.
@pytest.mark.tags("google-drive", "precheck")
@requires_env("GOOGLE_DRIVE_ID", "GOOGLE_DRIVE_SERVICE_KEY")
def test_google_drive_precheck_invalid_drive_id(google_drive_connection_config):
    invalid_drive_id = "invalid_drive_id"
    connection_config = GoogleDriveConnectionConfig(
        drive_id=invalid_drive_id,
        access_config=google_drive_connection_config.access_config,
    )
    index_config = GoogleDriveIndexerConfig(recursive=True)
    indexer = GoogleDriveIndexer(connection_config=connection_config, index_config=index_config)
    with pytest.raises(SourceConnectionError) as excinfo:
        indexer.precheck()
    assert "invalid" in str(excinfo.value).lower() or "not found" in str(excinfo.value).lower()


MIME_TYPES_TO_TEST = [
    "application/vnd.google-apps.document",
    "application/vnd.google-apps.spreadsheet",
    "application/vnd.google-apps.presentation",
    "application/vnd.google-apps.drawing",
]

EXPECTED_EXPORT_EXTENSIONS = {
    "application/vnd.google-apps.document": ".docx",
    "application/vnd.google-apps.spreadsheet": ".xlsx",
    "application/vnd.google-apps.presentation": ".pptx",
    "application/vnd.google-apps.drawing": ".png",
}

OPTIONAL_NATIVE_MIME_TYPES = {
    "application/vnd.google-apps.drawing",
}


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_mime", MIME_TYPES_TO_TEST)
@pytest.mark.tags(CONNECTOR_TYPE, "integration", "export")
@requires_env("GOOGLE_DRIVE_NATIVE_TEST_ID", "GOOGLE_DRIVE_SERVICE_KEY")
async def test_google_drive_export_by_type(expected_mime, temp_dir):
    """
    Parametrized test for verifying export of each specific Google-native format
    using the Drive API export endpoints.
    """

    drive_id = os.environ["GOOGLE_DRIVE_NATIVE_TEST_ID"]
    service_key = os.environ["GOOGLE_DRIVE_SERVICE_KEY"]

    connection_config = GoogleDriveConnectionConfig(
        drive_id=drive_id,
        access_config=GoogleDriveAccessConfig(service_account_key=service_key),
    )
    index_config = GoogleDriveIndexerConfig(recursive=True)
    download_config = GoogleDriveDownloaderConfig(download_dir=temp_dir)

    indexer = GoogleDriveIndexer(connection_config=connection_config, index_config=index_config)
    downloader = GoogleDriveDownloader(
        connection_config=connection_config, download_config=download_config
    )

    file_datas = list(indexer.run())

    # Filter only the target MIME type
    target_files = [f for f in file_datas if f.additional_metadata.get("mimeType") == expected_mime]
    if not target_files and expected_mime in OPTIONAL_NATIVE_MIME_TYPES:
        pytest.skip(f"No optional Google-native fixture found with MIME type: {expected_mime}")
    assert target_files, f"No files found with MIME type: {expected_mime}"

    for file_data in target_files:
        downloaded = downloader.run(file_data)
        out_path = downloaded["path"]

        assert out_path.exists(), f"{out_path} not found after download"
        assert out_path.stat().st_size > 0, f"{out_path} is empty"

        method = file_data.additional_metadata.get("download_method", "")
        assert method == "google_workspace_export", f"Unexpected download method: {method}"
        assert out_path.suffix == EXPECTED_EXPORT_EXTENSIONS[expected_mime]


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@requires_env("GOOGLE_DRIVE_SERVICE_KEY")
async def test_google_drive_e2e_source(temp_dir):
    """
    End-to-end integration test for the Google Drive connector, aligned with legacy shell test.
    """
    service_account_key = os.environ["GOOGLE_DRIVE_SERVICE_KEY"]

    drive_id = "1OQZ66OHBE30rNsNa7dweGLfRmXvkT_jr"

    connection_config = GoogleDriveConnectionConfig(
        drive_id=drive_id,
        access_config=GoogleDriveAccessConfig(service_account_key=service_account_key),
    )

    index_config = GoogleDriveIndexerConfig(
        recursive=True,
        extensions=["pdf", "docx"],
    )
    download_config = GoogleDriveDownloaderConfig(download_dir=temp_dir)

    indexer = GoogleDriveIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = GoogleDriveDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="google_drive_e2e",
            expected_num_files=4,  # adjust if fixture differs
            validate_downloaded_files=True,
            exclude_fields_extend=[
                "metadata.date_created",
                "metadata.date_modified",
                "additional_metadata.permissions",
            ],
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
        ),
    )
