from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import PrivateAttr, Secret

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import DestinationConnectionError, ValueError
from unstructured_ingest.processes.connectors.weaviate.weaviate import (
    WeaviateAccessConfig,
    WeaviateConnectionConfig,
    WeaviateUploader,
    WeaviateUploaderConfig,
    WeaviateUploadStager,
    WeaviateUploadStagerConfig,
)


class WeaviateConnectionConfigTest(WeaviateConnectionConfig):
    _mock_client: Any = PrivateAttr(default=None)

    @contextmanager
    def get_client(self):
        yield self._mock_client if self._mock_client is not None else MagicMock()

    def set_mock_client(self, mock_client: Any) -> None:
        self._mock_client = mock_client


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


@pytest.fixture
def file_data() -> FileData:
    return FileData(
        source_identifiers=SourceIdentifiers(fullpath="x.txt", filename="x.txt"),
        connector_type="weaviate",
        identifier="rec-123",
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


def _sample_element() -> dict:
    return {
        "type": "NarrativeText",
        "element_id": "abc",
        "text": "hello world",
        "metadata": {
            "languages": ["eng"],
            "filename": "smallfile.txt",
            "filetype": "text/plain",
            "page_number": 1,
            "data_source": {
                "url": "s3://bucket/key",
                "version": 12345,
                "record_locator": {"protocol": "s3", "remote_file_path": "s3://bucket/"},
                "date_created": "1778727504.0",
                "date_modified": "1778727504.0",
                "date_processed": "1778731882.6001136",
                "permissions_data": [{"role": "viewer"}],
            },
            "coordinates": {
                "system": "PixelSpace",
                "layout_width": 612,
                "layout_height": 792,
                "points": [[0, 0], [612, 0], [612, 792], [0, 792]],
            },
        },
    }


def test_conform_dict_default_keeps_metadata_nested(file_data: FileData):
    stager = WeaviateUploadStager(upload_stager_config=WeaviateUploadStagerConfig())
    out = stager.conform_dict(_sample_element(), file_data=file_data)
    assert "metadata" in out
    assert out["metadata"]["filename"] == "smallfile.txt"
    assert out["metadata"]["page_number"] == "1"
    assert out["metadata"]["data_source"]["version"] == "12345"
    assert isinstance(out["metadata"]["data_source"]["record_locator"], str)
    assert isinstance(out["metadata"]["coordinates"]["points"], str)
    assert out["record_id"] == "rec-123"


def test_conform_dict_flatten_produces_top_level_keys(file_data: FileData):
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True)
    )
    out = stager.conform_dict(_sample_element(), file_data=file_data)
    assert "metadata" not in out
    assert out["filename"] == "smallfile.txt"
    assert out["filetype"] == "text/plain"
    assert out["languages"] == ["eng"]
    assert out["page_number"] == "1"
    assert out["data_source_url"] == "s3://bucket/key"
    assert out["data_source_version"] == "12345"
    assert isinstance(out["data_source_record_locator"], str)
    assert isinstance(out["data_source_permissions_data"], str)
    assert out["coordinates_system"] == "PixelSpace"
    assert out["coordinates_layout_width"] == 612
    assert isinstance(out["coordinates_points"], str)
    assert out["data_source_date_created"].endswith("Z")
    assert out["record_id"] == "rec-123"
    assert out["element_id"] == "abc"
    assert out["text"] == "hello world"
    assert out["type"] == "NarrativeText"


def test_conform_dict_flatten_drops_none_metadata_values(file_data: FileData):
    element = _sample_element()
    element["metadata"]["sent_from"] = None
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True)
    )
    out = stager.conform_dict(element, file_data=file_data)
    assert "sent_from" not in out


def test_conform_to_schema_drops_unknown_and_fills_missing():
    schema_props = {"record_id", "text", "filename", "languages", "page_number"}
    row = {
        "record_id": "rec-1",
        "text": "hello",
        "filename": "x.txt",
        "languages": ["eng"],
        "junk_field": "should be dropped",
    }
    out = WeaviateUploader.conform_to_schema(row, schema_props)
    assert set(out.keys()) == schema_props
    assert out["page_number"] is None
    assert "junk_field" not in out
    assert out["languages"] == ["eng"]


def _make_property(name: str, nested_properties=None):
    prop = MagicMock()
    prop.name = name
    prop.nested_properties = nested_properties
    return prop


def _make_collection_config(*property_names: str, with_nested: bool = False):
    collection = MagicMock()
    config = MagicMock()
    if with_nested:
        config.properties = [
            _make_property("metadata", nested_properties=[_make_property("filename")]),
        ]
    else:
        config.properties = [_make_property(n) for n in property_names]
    collection.config.get.return_value = config
    return collection


def test_precheck_flatten_mode_collection_missing(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=True)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = False
    connection_config.set_mock_client(mock_client)

    with pytest.raises(DestinationConnectionError, match="must be pre-created"):
        uploader.precheck()


def test_precheck_flatten_mode_rejects_nested_schema(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=True)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = True
    mock_client.collections.get.return_value = _make_collection_config(with_nested=True)
    connection_config.set_mock_client(mock_client)

    with pytest.raises(DestinationConnectionError, match="nested object"):
        uploader.precheck()


def test_precheck_flatten_mode_requires_record_id_property(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=True)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = True
    mock_client.collections.get.return_value = _make_collection_config("text", "filename")
    connection_config.set_mock_client(mock_client)

    with pytest.raises(DestinationConnectionError, match="missing required property"):
        uploader.precheck()


def test_precheck_flatten_mode_passes_with_flat_schema(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=True)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = True
    mock_client.collections.get.return_value = _make_collection_config(
        "record_id", "text", "filename", "languages"
    )
    connection_config.set_mock_client(mock_client)

    uploader.precheck()


def test_create_destination_noop_in_flatten_mode(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=True)
    mock_client = MagicMock()
    connection_config.set_mock_client(mock_client)

    created = uploader.create_destination(destination_name="ignored")
    assert created is False
    mock_client.collections.create_from_dict.assert_not_called()


def test_get_schema_property_names_caches_across_calls(
    uploader: WeaviateUploader,
):
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=True)
    mock_client = MagicMock()
    mock_client.collections.get.return_value = _make_collection_config("record_id", "text")

    first = uploader.get_schema_property_names(mock_client)
    second = uploader.get_schema_property_names(mock_client)
    assert first == second == {"record_id", "text"}
    assert mock_client.collections.get.call_count == 1
