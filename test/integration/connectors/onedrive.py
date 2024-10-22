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
from unstructured_ingest.utils import requires_env

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
    SOURCE_TAG,
    env_setup_path,
)

@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
@requires_env(
    "ONEDRIVE_TEST_CLIENT_ID",
    "ONEDRIVE_TEST_CLIENT_SECRET",
    "ONEDRIVE_TEST_TENANT_ID",
    "ONEDRIVE_TEST_USER_PRINCIPAL_NAME",
)
async def test_onedrive_destination(upload_file: Path):
    """
    Integration test for the OneDrive destination connector.

    This test uploads a file to OneDrive and verifies that it exists.
    """

    # Retrieve credentials from environment variables
    client_id = os.environ["ONEDRIVE_TEST_CLIENT_ID"]
    client_secret = os.environ["ONEDRIVE_TEST_CLIENT_SECRET"]
    tenant_id = os.environ["ONEDRIVE_TEST_TENANT_ID"]
    user_pname = os.environ["ONEDRIVE_TEST_USER_PRINCIPAL_NAME"]

    # Generate a unique destination path for the test
    destination_folder = f"/destination/{uuid.uuid4()}"
    destination_fullpath = f"{destination_folder}/{upload_file.name}"

    # Configure the connection to OneDrive
    connection_config = OnedriveConnectionConfig(
        client_id=client_id,
        tenant=tenant_id,
        user_pname=user_pname,
        access_config=OnedriveAccessConfig(client_cred=client_secret),
    )

    # Configure the uploader (no additional settings needed for now)
    upload_config = OnedriveUploaderConfig()

    # Instantiate the uploader
    uploader = OnedriveUploader(
        connection_config=connection_config,
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

    try:
        # Run the uploader
        if uploader.is_async():
            await uploader.run_async(path=upload_file, file_data=file_data)
        else:
            uploader.run(path=upload_file, file_data=file_data)

        # Verify that the file was uploaded
        client = connection_config.get_client()
        drive = client.users[user_pname].drive

        # Attempt to retrieve the uploaded file
        uploaded_file = (
            drive.root.get_by_path(destination_fullpath)
            .select("id", "name")
            .get()
            .execute_query()
        )

        # Check if the file exists
        assert uploaded_file is not None
        assert uploaded_file.name == upload_file.name
        logger.info(f"File '{uploaded_file.name}' successfully uploaded to OneDrive.")

    finally:
        # Cleanup: Delete the uploaded file
        try:
            uploaded_file = drive.root.get_by_path(destination_fullpath)
            uploaded_file.delete_object().execute_query()
            logger.info(f"File '{destination_fullpath}' deleted from OneDrive.")
        except Exception as e:
            logger.error(f"Failed to delete file '{destination_fullpath}': {e}")