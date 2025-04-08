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


@pytest.mark.asyncio
@pytest.mark.tags("google-drive", "integration", "export")
@requires_env("GOOGLE_DRIVE_NATIVE_TEST_ID", "GOOGLE_DRIVE_SERVICE_KEY")
async def test_google_drive_native_formats_with_export_links(temp_dir):
    """
    Test that Google-native files and others are exported via exportLinks or webContentLink,
    bypassing size limits. All formats should be downloadable and leave behind valid local files.
    """
    from pathlib import Path

    drive_id = os.environ["GOOGLE_DRIVE_NATIVE_TEST_ID"]
    service_key = os.environ["GOOGLE_DRIVE_SERVICE_KEY"]

    connection_config = GoogleDriveConnectionConfig(
        drive_id=drive_id,
        access_config=GoogleDriveAccessConfig(service_account_key=service_key),
    )
    index_config = GoogleDriveIndexerConfig(recursive=True)
    download_config = GoogleDriveDownloaderConfig(download_dir=temp_dir)

    indexer = GoogleDriveIndexer(connection_config=connection_config, index_config=index_config)
    downloader = GoogleDriveDownloader(connection_config=connection_config, download_config=download_config)

    expected_types = {
        "application/vnd.google-apps.document": False,
        "application/vnd.google-apps.spreadsheet": False,
        "application/vnd.google-apps.presentation": False,
    }

    file_datas = list(indexer.run())
    assert len(file_datas) >= 3, f"Expected at least 3 files in test folder, got {len(file_datas)}"

    for file_data in file_datas:
        mime_type = file_data.additional_metadata.get("mimeType", "")
        if mime_type not in expected_types:
            continue

        downloaded = downloader.run(file_data)
        out_path = downloaded["path"]

        assert out_path.exists(), f"{out_path} not found after download"
        assert out_path.stat().st_size > 0, f"{out_path} is empty"

        method = file_data.additional_metadata.get("download_method", "")
        assert method in {"export_link", "web_content_link"}, f"Unexpected download method: {method}"

        expected_types[mime_type] = True

    assert all(expected_types.values()), f"Did not successfully test all expected MIME types: {expected_types}"
