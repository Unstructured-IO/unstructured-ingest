import os
import uuid
from pathlib import Path

import pytest

from unstructured_ingest.v2.processes.connectors.onedrive import (
    OnedriveAccessConfig,
    OnedriveConnectionConfig,
    OnedriveUploader,
    OnedriveUploaderConfig,
    CONNECTOR_TYPE,
)
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.logger import logger
from test.integration.utils import requires_env

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
    env_setup_path,
)

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

    # Configure the uploader (no additional settings needed for now)
    upload_config = OnedriveUploaderConfig()

    # Instantiate the uploader
    uploader = OnedriveUploader(
        connection_config=onedrive_connection_config,
        upload_config=upload_config,
    )

    # Prepare the FileData object
    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=destination_fullpath,
            filename=upload_file.name,
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="mock_file_data",
    )

    # Run the uploader
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