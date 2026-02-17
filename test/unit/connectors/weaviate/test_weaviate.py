import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import Secret

from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.weaviate.weaviate import (
    WeaviateAccessConfig,
    WeaviateConnectionConfig,
    WeaviateUploader,
    WeaviateUploaderConfig,
    WeaviateUploadStagerConfig,
)


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


def test_upload_stager_config_default():
    """Test that enable_lexical_search defaults to False"""
    config = WeaviateUploadStagerConfig()
    assert config.enable_lexical_search is False


def test_upload_stager_config_enabled():
    """Test that enable_lexical_search can be enabled"""
    config = WeaviateUploadStagerConfig(enable_lexical_search=True)
    assert config.enable_lexical_search is True


def test_collection_config_has_bm25():
    """Test that the collection config includes BM25 configuration"""
    # Get the path to the config file
    weaviate_py = Path(__file__).parents[4] / "unstructured_ingest" / "processes" / "connectors" / "weaviate" / "weaviate.py"
    connectors_dir = weaviate_py.parents[1]
    collection_config_file = connectors_dir / "assets" / "weaviate_collection_config.json"

    with collection_config_file.open() as f:
        collection_config = json.load(f)

    # Verify BM25 configuration exists
    assert "invertedIndexConfig" in collection_config
    assert "bm25" in collection_config["invertedIndexConfig"]

    bm25_config = collection_config["invertedIndexConfig"]["bm25"]
    assert "b" in bm25_config
    assert "k1" in bm25_config
    assert bm25_config["b"] == 0.75
    assert bm25_config["k1"] == 1.2


def test_collection_config_text_field_searchable():
    """Test that the text field is configured for lexical search"""
    # Get the path to the config file
    weaviate_py = Path(__file__).parents[4] / "unstructured_ingest" / "processes" / "connectors" / "weaviate" / "weaviate.py"
    connectors_dir = weaviate_py.parents[1]
    collection_config_file = connectors_dir / "assets" / "weaviate_collection_config.json"

    with collection_config_file.open() as f:
        collection_config = json.load(f)

    # Find the text property
    text_property = next(
        (prop for prop in collection_config["properties"] if prop["name"] == "text"),
        None
    )

    assert text_property is not None
    assert text_property["indexSearchable"] is True
    assert text_property["tokenization"] == "word"
