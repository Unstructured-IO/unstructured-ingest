import os
import json
import uuid
import pytest
from googleapiclient.errors import HttpError

from test.integration.connectors.utils.constants import (
    BLOB_STORAGE_TAG,
    DESTINATION_TAG,
    SOURCE_TAG,
)
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.google_drive import (
    CONNECTOR_TYPE,
    GoogleDriveAccessConfig,
    GoogleDriveConnectionConfig,
    GoogleDriveDownloader,
    GoogleDriveDownloaderConfig,
    GoogleDriveIndexer,
    GoogleDriveIndexerConfig,
)
from unstructured_ingest.error import (
    SourceConnectionError,
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

@pytest.mark.asyncio
@pytest.mark.tags("google-drive", "source", "cloud")
@requires_env("GOOGLE_DRIVE_ID", "GOOGLE_DRIVE_SERVICE_KEY")
async def test_google_drive_source(temp_dir, google_drive_connection_config):

    index_config = GoogleDriveIndexerConfig(recursive=True, extensions=["pdf", "docx"])
    download_config = GoogleDriveDownloaderConfig(download_dir=temp_dir)

    indexer = GoogleDriveIndexer(
        connection_config=google_drive_connection_config,
        index_config=index_config,
    )
    downloader = GoogleDriveDownloader(
        connection_config=google_drive_connection_config,
        download_config=download_config,
    )

    # This common validation will first call precheck(), then run the indexing and downloading.
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="google_drive",
            expected_num_files=4,
            exclude_fields_extend=[
                "metadata.date_created",
                "metadata.date_modified",
            ],
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
    assert ("invalid" in str(excinfo.value).lower() or "not found" in str(excinfo.value).lower())


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
    assert ("invalid" in str(excinfo.value).lower() or "not found" in str(excinfo.value).lower())


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
    assert ("forbidden" in str(excinfo.value).lower() or "permission" in str(excinfo.value).lower())


# Precheck fails when the folder is empty.
@pytest.mark.tags("google-drive", "precheck")
@requires_env("GOOGLE_DRIVE_ID", "GOOGLE_DRIVE_SERVICE_KEY")
def test_google_drive_precheck_empty_folder(google_drive_connection_config, google_drive_empty_folder):
    # Use the empty folder's ID as the target.
    connection_config = GoogleDriveConnectionConfig(
        drive_id=google_drive_empty_folder,
        access_config=google_drive_connection_config.access_config,
    )
    index_config = GoogleDriveIndexerConfig(recursive=True)
    indexer = GoogleDriveIndexer(connection_config=connection_config, index_config=index_config)
    with pytest.raises(SourceConnectionError) as excinfo:
        indexer.precheck()
    assert "empty" in str(excinfo.value).lower()