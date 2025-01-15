import os
import uuid
from pathlib import Path

import pytest
from office365.graph_client import GraphClient

from test.integration.connectors.utils.constants import BLOB_STORAGE_TAG, DESTINATION_TAG
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.onedrive import (
    CONNECTOR_TYPE,
    OnedriveAccessConfig,
    OnedriveConnectionConfig,
    OnedriveUploader,
    OnedriveUploaderConfig,
)


@pytest.fixture
@pytest.mark.xfail(
    reason="Issues with test setup on the provider side."
)  # TODO: remove line when issues are addressed
def onedrive_test_folder() -> str:
    """
    Pytest fixture that creates a test folder in OneDrive and deletes it after test run.
    """
    connection_config = get_connection_config()
    user_pname = connection_config.user_pname

    # Get the OneDrive client
    client: GraphClient = connection_config.get_client()
    drive = client.users[user_pname].drive

    # Generate a unique test folder path
    test_folder_path = f"utic-test-output-{uuid.uuid4()}"

    # Create the test folder
    root = drive.root
    folder = root.create_folder(test_folder_path).execute_query()
    print(f"created folder: {folder.name}")
    try:
        yield test_folder_path
    finally:
        # Teardown: delete the test folder and its contents
        folder.delete_object().execute_query()
        print(f"successfully deleted folder: {folder.name}")


def get_connection_config():
    """
    Pytest fixture that provides the OnedriveConnectionConfig for tests.
    """
    client_id = os.getenv("MS_CLIENT_ID")
    client_secret = os.getenv("MS_CLIENT_CRED")
    tenant_id = os.getenv("MS_TENANT_ID")
    user_pname = os.getenv("MS_USER_PNAME")

    connection_config = OnedriveConnectionConfig(
        client_id=client_id,
        tenant=tenant_id,
        user_pname=user_pname,
        access_config=OnedriveAccessConfig(client_cred=client_secret),
    )
    return connection_config


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, BLOB_STORAGE_TAG)
@requires_env("MS_CLIENT_CRED", "MS_CLIENT_ID", "MS_TENANT_ID", "MS_USER_PNAME")
@pytest.mark.xfail(
    reason="Issues with test setup on the provider side."
)  # TODO: remove line when issues are addressed
def test_onedrive_destination(upload_file: Path, onedrive_test_folder: str):
    """
    Integration test for the OneDrive destination connector.

    This test uploads a file to OneDrive and verifies that it exists.
    """
    connection_config = get_connection_config()
    # Retrieve user principal name from the connection config
    user_pname = connection_config.user_pname

    # The test folder is provided by the fixture
    destination_folder = onedrive_test_folder
    destination_fullpath = f"{destination_folder}/{upload_file.name}"

    # Configure the uploader with remote_url
    upload_config = OnedriveUploaderConfig(remote_url=f"onedrive://{destination_folder}")

    uploader = OnedriveUploader(
        connection_config=connection_config,
        upload_config=upload_config,
    )

    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=destination_fullpath,
            filename=upload_file.name,
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="mock_file_data",
    )
    uploader.precheck()
    uploader.run(path=upload_file, file_data=file_data)

    # Verify that the file was uploaded
    client = connection_config.get_client()
    drive = client.users[user_pname].drive

    uploaded_file = (
        drive.root.get_by_path(destination_fullpath).select(["id", "name"]).get().execute_query()
    )

    # Check if the file exists
    assert uploaded_file is not None
    assert uploaded_file.name == upload_file.name
