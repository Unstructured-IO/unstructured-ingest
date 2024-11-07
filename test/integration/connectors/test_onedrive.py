import os
import uuid
from pathlib import Path

import pytest

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
)
from test.integration.utils import requires_env
from unstructured_ingest.logger import logger
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.onedrive import (
    CONNECTOR_TYPE,
    OnedriveAccessConfig,
    OnedriveConnectionConfig,
    OnedriveUploader,
    OnedriveUploaderConfig,
)


@pytest.fixture
def onedrive_test_folder(onedrive_connection_config):
    """
    Pytest fixture that creates a test folder in OneDrive and deletes it after test run.
    """
    user_pname = onedrive_connection_config.user_pname

    # Get the OneDrive client
    client = onedrive_connection_config.get_client()
    drive = client.users[user_pname].drive

    # Generate a unique test folder path
    test_folder_name = str(uuid.uuid4())
    test_folder_path = f"/onedrive_destination_test/{test_folder_name}"

    # Create the test folder
    try:
        # Starting from root
        root_folder = drive.root

        # Split the path and create folders as necessary
        folder_parts = test_folder_path.strip("/").split("/")
        current_folder = root_folder

        for part in folder_parts:
            try:
                current_folder = current_folder.children[part].get().execute_query()
            except Exception:
                # Folder doesn't exist, create it using create_folder()
                current_folder = current_folder.create_folder(part).execute_query()
    except Exception as e:
        pytest.fail(f"Failed to create test folder '{test_folder_path}': {e}")

    yield test_folder_path

    # Teardown: delete the test folder and its contents
    try:
        folder_item = drive.root.get_by_path(test_folder_path)
        folder_item.delete_object().execute_query()
        print(f"Test folder '{test_folder_path}' deleted from OneDrive.")
    except Exception as e:
        print(f"Failed to delete test folder '{test_folder_path}': {e}")


@pytest.fixture(scope="session")
@requires_env("MS_CLIENT_CRED", "MS_CLIENT_ID", "MS_TENANT_ID", "MS_USER_PNAME")
def onedrive_connection_config():
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


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
def test_onedrive_destination(upload_file: Path, onedrive_connection_config, onedrive_test_folder):
    """
    Integration test for the OneDrive destination connector.

    This test uploads a file to OneDrive and verifies that it exists.
    """

    # Retrieve user principal name from the connection config
    user_pname = onedrive_connection_config.user_pname

    # The test folder is provided by the fixture
    destination_folder = onedrive_test_folder
    destination_fullpath = f"{destination_folder}/{upload_file.name}"

    # Configure the uploader with remote_url
    upload_config = OnedriveUploaderConfig(remote_url=f"onedrive://{destination_folder}")

    uploader = OnedriveUploader(
        connection_config=onedrive_connection_config,
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

    uploader.run(path=upload_file, file_data=file_data)

    # Verify that the file was uploaded
    client = onedrive_connection_config.get_client()
    drive = client.users[user_pname].drive

    # Attempt to retrieve the uploaded file
    try:
        uploaded_file = (
            drive.root.get_by_path(destination_fullpath)
            .select(["id", "name"])
            .get()
            .execute_query()
        )

        # Check if the file exists
        assert uploaded_file is not None
        assert uploaded_file.name == upload_file.name
        logger.info(f"File '{uploaded_file.name}' successfully uploaded to OneDrive.")
    except Exception as e:
        pytest.fail(f"Failed to verify uploaded file '{destination_fullpath}': {e}")
