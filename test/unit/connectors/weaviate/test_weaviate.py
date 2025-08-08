from unittest.mock import MagicMock

import pytest
from pydantic import Secret

from unstructured_ingest.processes.connectors.weaviate.weaviate import (
    WeaviateAccessConfig,
    WeaviateConnectionConfig,
    WeaviateUploader,
    WeaviateUploaderConfig,
)
from unstructured_ingest.errors_v2 import ValueError


class WeaviateConnectionConfigTest(WeaviateConnectionConfig):
    def get_client(self):
        yield MagicMock()


@pytest.fixture
def access_config():
    return WeaviateAccessConfig()


@pytest.fixture
def connection_config(access_config: WeaviateAccessConfig):
    return WeaviateConnectionConfigTest(
        access_config=Secret(access_config),
        init_timeout=10,
        query_timeout=10,
        insert_timeout=10,
    )


@pytest.fixture
def uploader_config():
    return WeaviateUploaderConfig(collection=None)


@pytest.fixture
def uploader(
    connection_config: WeaviateConnectionConfigTest, uploader_config: WeaviateUploaderConfig
):
    return WeaviateUploader(
        connection_config=connection_config,
        upload_config=uploader_config,
        connector_type="weaviate",
    )


@pytest.mark.parametrize(
    ("destination_name", "expected"),
    [
        ("t", "T"),
        ("test123", "Test123"),
        ("test__ __", "Test_____"),
        ("test-Name", "Test_Name"),
        ("teSt name", "TeSt_name"),
        ("test@name#123", "Test_name_123"),
    ],
)
def test_format_destination_name_success_logs(
    caplog: pytest.LogCaptureFixture,
    uploader: WeaviateUploader,
    destination_name: str,
    expected: str,
):
    formatted_name = uploader.format_destination_name(destination_name)
    assert formatted_name == expected
    assert len(caplog.records) == 1
    assert (
        f"Given Collection name '{destination_name}' doesn't follow naming conventions. "
        f"Renaming to '{expected}'"
    ) in caplog.text


@pytest.mark.parametrize(
    ("destination_name", "expected"),
    [
        ("T", "T"),
        ("Test_123", "Test_123"),
        ("TEST_NAME", "TEST_NAME"),
    ],
)
def test_format_destination_name_success_no_logs(
    caplog: pytest.LogCaptureFixture,
    uploader: WeaviateUploader,
    destination_name: str,
    expected: str,
):
    formatted_name = uploader.format_destination_name(destination_name)
    assert formatted_name == expected
    assert len(caplog.records) == 0


@pytest.mark.parametrize(
    ("destination_name"),
    [
        ("123name"),
        ("@#$%^&*"),
        (""),
    ],
)
def test_format_destination_name_error(uploader: WeaviateUploader, destination_name: str):
    with pytest.raises(ValueError):
        uploader.format_destination_name(destination_name)
