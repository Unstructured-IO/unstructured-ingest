import os
import tempfile
import uuid
from pathlib import Path
from typing import Generator

import pytest

from test.integration.utils import requires_env
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connectors.onedrive import (
    OnedriveAccessConfig,
    OnedriveConnectionConfig,
)

FILENAME = "DA-1p-with-duplicate-pages.pdf.json"


@pytest.fixture
def upload_file() -> Path:
    int_test_dir = Path(__file__).parent
    assets_dir = int_test_dir / "assets"
    upload_file = assets_dir / FILENAME
    assert upload_file.exists()
    assert upload_file.is_file()
    return upload_file


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        logger.info(f"Created temp dir '{temp_path}'")
        yield temp_path
        logger.info(f"Removing temp dir '{temp_path}'")


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
    client_id = os.environ["MS_CLIENT_ID"]
    client_secret = os.environ["MS_CLIENT_CRED"]
    tenant_id = os.environ["MS_TENANT_ID"]
    user_pname = os.environ["MS_USER_PNAME"]

    connection_config = OnedriveConnectionConfig(
        client_id=client_id,
        tenant=tenant_id,
        user_pname=user_pname,
        access_config=OnedriveAccessConfig(client_cred=client_secret),
    )
    return connection_config
