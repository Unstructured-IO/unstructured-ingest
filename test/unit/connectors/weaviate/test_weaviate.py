import json
from contextlib import contextmanager
from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import PrivateAttr, Secret

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import DestinationConnectionError, ValueError, WriteError
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
    """Representative element shape covering every metadata branch the stager
    transforms in default mode and flattens in flatten mode. Tests assert on
    the keys they care about; extra keys are ignored."""
    return {
        "type": "NarrativeText",
        "element_id": "abc",
        "text": "hello world",
        "metadata": {
            "languages": ["eng"],
            "filename": "smallfile.txt",
            "filetype": "text/plain",
            "page_number": 1,
            "last_modified": "2025-01-08T22:45:32",
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
            "links": [{"text": "click", "url": "https://example.com", "start_index": 0}],
            "regex_metadata": {
                "phone": [{"text": "555-1234", "start": 0, "end": 8}],
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
    """Flatten mode is a pure flatten: dicts recurse, lists pass through raw, no
    opinionated coercion (no str-casts, no json.dumps, no date reformatting)."""
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True)
    )
    out = stager.conform_dict(_sample_element(), file_data=file_data)
    assert "metadata" not in out

    assert out["filename"] == "smallfile.txt"
    assert out["filetype"] == "text/plain"
    assert out["languages"] == ["eng"]

    assert out["page_number"] == 1
    assert out["data_source_url"] == "s3://bucket/key"
    assert out["data_source_version"] == 12345

    assert "data_source_record_locator" not in out
    assert out["data_source_record_locator_protocol"] == "s3"
    assert out["data_source_record_locator_remote_file_path"] == "s3://bucket/"

    assert out["data_source_permissions_data"] == [{"role": "viewer"}]
    assert out["coordinates_system"] == "PixelSpace"
    assert out["coordinates_layout_width"] == 612
    assert out["coordinates_points"] == [[0, 0], [612, 0], [612, 792], [0, 792]]

    assert out["data_source_date_created"] == "1778727504.0"

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


def test_conform_dict_flatten_auto_schema_normalizes_then_flattens(file_data: FileData):
    """With auto_schema, flatten mode applies the same value normalization as the
    non-flatten path (stringify 2-D arrays / dicts / list[dict], format dates, cast
    version/page_number) BEFORE flattening, so every value is Weaviate-typeable —
    unlike pure flatten which leaves e.g. coordinates.points as a 2-D array that
    Weaviate cannot type."""
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True, auto_schema=True)
    )
    out = stager.conform_dict(_sample_element(), file_data=file_data)

    assert "metadata" not in out  # still flattened to top level

    # 2-D array / dict / list[dict] fields become strings, not raw structures
    assert out["coordinates_points"] == "[[0, 0], [612, 0], [612, 792], [0, 792]]"
    assert isinstance(out["data_source_permissions_data"], str)
    assert isinstance(out["links"], str)
    assert isinstance(out["regex_metadata"], str)

    # record_locator collapses to a single string column, not proliferated sub-keys
    assert isinstance(out["data_source_record_locator"], str)
    assert "data_source_record_locator_protocol" not in out

    # scalars cast to strings; dates formatted RFC3339
    assert out["data_source_version"] == "12345"
    assert out["page_number"] == "1"
    assert out["last_modified"].endswith("Z") and "T" in out["last_modified"]

    # plain 1-D arrays remain arrays (Weaviate text[])
    assert out["languages"] == ["eng"]
    assert out["record_id"] == "rec-123"


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
    properties = [_make_property(n) for n in property_names]
    if with_nested:
        # Mirrors what a user declares as OBJECT_ARRAY for list[dict] fields
        # (e.g. permissions_data, links, regex_metadata_<pattern>).
        properties.append(
            _make_property(
                "data_source_permissions_data",
                nested_properties=[_make_property("role")],
            )
        )
    config.properties = properties
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


def test_precheck_flatten_mode_accepts_nested_schema(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    """Schema-shape (e.g. OBJECT_ARRAY for list[dict] fields like permissions_data)
    is the user's call. Precheck only enforces collection existence and that
    record_id is declared, regardless of how other properties are typed."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=True)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = True
    mock_client.collections.get.return_value = _make_collection_config(
        "record_id", "text", with_nested=True
    )
    connection_config.set_mock_client(mock_client)

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


def test_create_destination_default_mode_creates_when_absent(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    """Default mode (auto_schema disabled, non-flatten) seeds the collection from
    the default asset config when it does not exist yet (pre-declaring
    metadata.data_source.version as text) and formats the name."""
    uploader.upload_config = WeaviateUploaderConfig(collection="my-coll", auto_schema=False)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = False
    connection_config.set_mock_client(mock_client)

    created = uploader.create_destination()
    assert created is True
    assert uploader.upload_config.collection == "My_coll"
    mock_client.collections.create_from_dict.assert_called_once()


def test_create_destination_default_mode_skips_when_present(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    """Default mode does not recreate an existing collection."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", auto_schema=False)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = True
    connection_config.set_mock_client(mock_client)

    created = uploader.create_destination()
    assert created is False
    mock_client.collections.create_from_dict.assert_not_called()


def test_create_destination_resolves_fallback_name(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    """With no collection set, create_destination falls back to the default name."""
    uploader.upload_config = WeaviateUploaderConfig(collection=None, auto_schema=False)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = True
    connection_config.set_mock_client(mock_client)

    uploader.create_destination()
    assert uploader.upload_config.collection == "Unstructuredautocreated"


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


def test_collection_present_caches_positive_result(uploader: WeaviateUploader):
    """Once the collection is known to exist, the result is cached and the
    existence round-trip is not repeated on subsequent uploads."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", auto_schema=True)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = True

    assert uploader._collection_present(mock_client) is True
    assert uploader._collection_present(mock_client) is True
    mock_client.collections.exists.assert_called_once()


def test_collection_present_rechecks_until_it_exists(uploader: WeaviateUploader):
    """A negative result is not cached — in auto_schema mode Weaviate creates the
    collection on first insert, so it can flip from absent to present mid-run."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", auto_schema=True)
    mock_client = MagicMock()
    mock_client.collections.exists.side_effect = [False, True]

    assert uploader._collection_present(mock_client) is False
    assert uploader._collection_present(mock_client) is True
    assert mock_client.collections.exists.call_count == 2


# ---------------------------------------------------------------------------
# Stager — nested (default) mode: deeper coverage of every transform branch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "expected_json_payload"),
    [
        # dict → JSON string
        (
            ("data_source", "record_locator"),
            {"protocol": "s3", "remote_file_path": "s3://bucket/"},
        ),
        # list[list[*]] → JSON string (no native Weaviate scalar)
        (
            ("coordinates", "points"),
            [[0, 0], [612, 0], [612, 792], [0, 792]],
        ),
        # list[dict] → JSON string
        (
            ("links",),
            [{"text": "click", "url": "https://example.com", "start_index": 0}],
        ),
        # list[dict] → JSON string
        (
            ("data_source", "permissions_data"),
            [{"role": "viewer"}],
        ),
        # dict-of-list-of-dict → JSON string
        (
            ("regex_metadata",),
            {"phone": [{"text": "555-1234", "start": 0, "end": 8}]},
        ),
    ],
)
def test_conform_dict_default_json_dumps_complex_metadata(
    file_data: FileData, path: tuple, expected_json_payload
):
    """Default mode applies json.dumps to dict / list[dict] / list[list[*]]
    fields that don't map to a native Weaviate scalar type."""
    stager = WeaviateUploadStager(upload_stager_config=WeaviateUploadStagerConfig())
    out = stager.conform_dict(_sample_element(), file_data=file_data)
    node = out["metadata"]
    for key in path:
        node = node[key]
    assert isinstance(node, str)
    assert json.loads(node) == expected_json_payload


@pytest.mark.parametrize(
    "ds_key",
    ["date_created", "date_modified", "date_processed"],
)
def test_conform_dict_default_reformats_unix_timestamp_dates(file_data: FileData, ds_key: str):
    """Default mode parses unix-timestamp-strings ("1778727504.0") and
    reformats them to Weaviate's RFC3339-Z form (YYYY-MM-DDTHH:MM:SS.fffZ)."""
    stager = WeaviateUploadStager(upload_stager_config=WeaviateUploadStagerConfig())
    out = stager.conform_dict(_sample_element(), file_data=file_data)
    node = out["metadata"]["data_source"][ds_key]
    assert isinstance(node, str)
    assert node.endswith("Z")
    # YYYY-MM-DDTHH:MM:SS.ffffffZ shape sanity check
    assert node[4] == "-" and node[7] == "-" and node[10] == "T"


def test_conform_dict_default_reformats_iso_dates_to_rfc3339(file_data: FileData):
    """Default mode also parses ISO strings like "2025-01-08T22:45:32" via
    dateutil and emits the same RFC3339-Z form."""
    stager = WeaviateUploadStager(upload_stager_config=WeaviateUploadStagerConfig())
    out = stager.conform_dict(_sample_element(), file_data=file_data)
    last_modified = out["metadata"]["last_modified"]
    assert last_modified.startswith("2025-01-08T22:45:32")
    assert last_modified.endswith("Z")


def test_conform_dict_default_str_casts_version_and_page_number(file_data: FileData):
    """Default mode coerces ints to TEXT-compatible strings to match the
    auto-created collection schema (which declares them as text)."""
    stager = WeaviateUploadStager(upload_stager_config=WeaviateUploadStagerConfig())
    out = stager.conform_dict(_sample_element(), file_data=file_data)
    assert out["metadata"]["data_source"]["version"] == "12345"
    assert out["metadata"]["page_number"] == "1"


def test_conform_dict_default_handles_missing_metadata(file_data: FileData):
    """Default mode tolerates elements with no metadata key — record_id is
    still injected and no transforms blow up on missing nested paths."""
    stager = WeaviateUploadStager(upload_stager_config=WeaviateUploadStagerConfig())
    out = stager.conform_dict({"text": "hi"}, file_data=file_data)
    assert out == {"text": "hi", "record_id": "rec-123"}


# ---------------------------------------------------------------------------
# Stager — flatten mode (pure flatten): edge cases
# ---------------------------------------------------------------------------


def test_conform_dict_flatten_handles_missing_metadata_key(file_data: FileData):
    """If an element has no metadata dict at all, flatten is a no-op and
    record_id is still injected."""
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True)
    )
    out = stager.conform_dict({"text": "hi", "element_id": "x"}, file_data=file_data)
    assert out == {"text": "hi", "element_id": "x", "record_id": "rec-123"}


def test_conform_dict_flatten_handles_empty_metadata_dict(file_data: FileData):
    """An element with an empty metadata dict produces no flattened keys."""
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True)
    )
    out = stager.conform_dict(
        {"text": "hi", "element_id": "x", "metadata": {}}, file_data=file_data
    )
    assert out == {"text": "hi", "element_id": "x", "record_id": "rec-123"}


def test_conform_dict_flatten_overwrites_record_id_from_element(file_data: FileData):
    """If an element somehow already has a record_id at the top level, the
    stager overrides it with file_data.identifier — record_id is the stager's
    contract, not user-controlled."""
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True)
    )
    element = {"text": "hi", "record_id": "OLD-ID", "metadata": {}}
    out = stager.conform_dict(element, file_data=file_data)
    assert out["record_id"] == "rec-123"


def test_conform_dict_flatten_preserves_falsy_primitive_values(file_data: FileData):
    """Falsy primitives (0, "", False) must survive flatten; only None is
    dropped (via remove_none=True)."""
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True)
    )
    element = {
        "text": "hi",
        "metadata": {
            "page_number": 0,
            "filename": "",
            "is_continuation": False,
            "languages": [],
            "skipped": None,
        },
    }
    out = stager.conform_dict(element, file_data=file_data)
    assert out["page_number"] == 0
    assert out["filename"] == ""
    assert out["is_continuation"] is False
    assert out["languages"] == []
    assert "skipped" not in out


def test_conform_dict_flatten_recursive_flatten_of_dynamic_keys(file_data: FileData):
    """Dynamic dict keys (like regex_metadata pattern names) recursively flatten
    into their own top-level keys; their list[dict] values pass through raw,
    enabling users to declare them as OBJECT_ARRAY in Weaviate."""
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True)
    )
    out = stager.conform_dict(_sample_element(), file_data=file_data)
    assert "regex_metadata" not in out
    assert out["regex_metadata_phone"] == [{"text": "555-1234", "start": 0, "end": 8}]


def test_conform_dict_flatten_does_not_mutate_input(file_data: FileData):
    """Flatten mode reads metadata via pop on a shallow copy and never mutates
    the inner metadata dict — callers can safely keep references to the input."""
    element = _sample_element()
    snapshot = deepcopy(element)
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True)
    )
    stager.conform_dict(element, file_data=file_data)
    assert element == snapshot


def test_conform_dict_flatten_passthrough_lists_unmodified(file_data: FileData):
    """list[primitive], list[dict], and list[list[*]] all pass through raw
    (flatten_lists=False). No json.dumps, no element-level reshaping."""
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(flatten_metadata=True)
    )
    out = stager.conform_dict(_sample_element(), file_data=file_data)
    assert out["languages"] == ["eng"]
    assert out["links"] == [{"text": "click", "url": "https://example.com", "start_index": 0}]
    assert out["data_source_permissions_data"] == [{"role": "viewer"}]
    assert out["coordinates_points"] == [[0, 0], [612, 0], [612, 792], [0, 792]]


# ---------------------------------------------------------------------------
# Uploader — conform_to_schema: edge cases
# ---------------------------------------------------------------------------


def test_conform_to_schema_empty_schema_returns_empty_dict():
    """If the schema has no properties, every incoming key is dropped."""
    out = WeaviateUploader.conform_to_schema({"a": 1, "b": 2}, schema_props=set())
    assert out == {}


def test_conform_to_schema_empty_properties_fills_all_with_none():
    """If incoming properties is empty, every schema field is filled with None."""
    out = WeaviateUploader.conform_to_schema({}, schema_props={"a", "b", "c"})
    assert out == {"a": None, "b": None, "c": None}


def test_conform_to_schema_preserves_falsy_values():
    """Falsy values (0, "", False, []) must not be confused with missing keys."""
    out = WeaviateUploader.conform_to_schema(
        {"i": 0, "s": "", "b": False, "lst": []},
        schema_props={"i", "s", "b", "lst", "missing"},
    )
    assert out == {"i": 0, "s": "", "b": False, "lst": [], "missing": None}


def test_conform_to_schema_preserves_explicit_none():
    """An explicit None in incoming and a missing key both produce None
    in the output — Weaviate stores them identically as null."""
    out = WeaviateUploader.conform_to_schema(
        {"explicit": None}, schema_props={"explicit", "missing"}
    )
    assert out == {"explicit": None, "missing": None}


# ---------------------------------------------------------------------------
# Uploader — precheck: default mode + edge cases
# ---------------------------------------------------------------------------


def test_precheck_default_mode_collection_missing_raises(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=False)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = False
    connection_config.set_mock_client(mock_client)

    with pytest.raises(DestinationConnectionError, match="does not exist"):
        uploader.precheck()


def test_precheck_default_mode_collection_exists_passes(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    """Default mode is permissive: existence is enough — no schema-shape or
    record_id checks are run because the connector may auto-create later."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=False)
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = True
    connection_config.set_mock_client(mock_client)

    uploader.precheck()
    # In default mode we never read the schema config.
    mock_client.collections.get.assert_not_called()


def test_precheck_default_mode_collection_none_skips_validation(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
):
    """In default mode, if no collection name is set precheck silently passes —
    the missing name is resolved later in create_destination (fallback to
    'Unstructuredautocreated')."""
    uploader.upload_config = WeaviateUploaderConfig(collection=None, flatten_metadata=False)
    mock_client = MagicMock()
    connection_config.set_mock_client(mock_client)

    uploader.precheck()
    mock_client.collections.exists.assert_not_called()


def test_precheck_flatten_mode_collection_none_raises(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
):
    """Flatten mode requires an explicit collection name because there is no
    auto-create fallback — fail early with a clear message, before even
    opening a client connection."""
    uploader.upload_config = WeaviateUploaderConfig(collection=None, flatten_metadata=True)
    mock_client = MagicMock()
    connection_config.set_mock_client(mock_client)

    with pytest.raises(DestinationConnectionError, match="requires an explicit collection name"):
        uploader.precheck()
    # Config validation fires before the client is opened.
    mock_client.collections.exists.assert_not_called()


def test_precheck_wraps_unexpected_exception_in_destination_error(
    uploader: WeaviateUploader, connection_config: WeaviateConnectionConfigTest
):
    """Any non-DestinationConnectionError exception bubbling from the client
    is wrapped in a DestinationConnectionError so callers always see one."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl")
    mock_client = MagicMock()
    mock_client.collections.exists.side_effect = RuntimeError("network down")
    connection_config.set_mock_client(mock_client)

    with pytest.raises(DestinationConnectionError, match="failed to validate connection"):
        uploader.precheck()


# ---------------------------------------------------------------------------
# Uploader — delete_by_record_id
# ---------------------------------------------------------------------------


def _delete_resp(failed: int, successful: int) -> MagicMock:
    return MagicMock(failed=failed, successful=successful)


def test_delete_by_record_id_loops_until_no_more_results(
    uploader: WeaviateUploader, file_data: FileData
):
    """The delete loop re-runs until a response with failed=0 AND
    successful=0 is observed (Weaviate's QUERY_MAXIMUM_RESULTS limit
    means a single delete may not clear everything)."""
    pytest.importorskip("weaviate")
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl")
    mock_client = MagicMock()
    delete_mock = mock_client.collections.get.return_value.data.delete_many
    delete_mock.side_effect = [
        _delete_resp(failed=0, successful=10),
        _delete_resp(failed=0, successful=5),
        _delete_resp(failed=0, successful=0),
    ]

    uploader.delete_by_record_id(client=mock_client, file_data=file_data)
    assert delete_mock.call_count == 3


def test_delete_by_record_id_raises_on_failure_response(
    uploader: WeaviateUploader, file_data: FileData
):
    """A response with failed > 0 raises immediately, preserving the
    original record id in the message for debugging."""
    pytest.importorskip("weaviate")
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl")
    mock_client = MagicMock()
    delete_mock = mock_client.collections.get.return_value.data.delete_many
    delete_mock.return_value = _delete_resp(failed=2, successful=0)

    with pytest.raises(WriteError, match="failed to delete records"):
        uploader.delete_by_record_id(client=mock_client, file_data=file_data)


# ---------------------------------------------------------------------------
# Uploader — check_for_errors
# ---------------------------------------------------------------------------


def test_check_for_errors_no_failures_passes(uploader: WeaviateUploader):
    mock_client = MagicMock()
    mock_client.batch.failed_objects = []

    uploader.check_for_errors(client=mock_client)


def test_check_for_errors_with_failures_raises_write_error(
    uploader: WeaviateUploader, caplog: pytest.LogCaptureFixture
):
    mock_client = MagicMock()
    mock_client.batch.failed_objects = [
        MagicMock(original_uuid="uuid-1", message="rejected by collection"),
        MagicMock(original_uuid="uuid-2", message="schema mismatch"),
    ]

    with pytest.raises(WriteError, match="Failed to upload to weaviate"):
        uploader.check_for_errors(client=mock_client)
    # Each failure produces an ERROR log line for operator visibility.
    assert "uuid-1" in caplog.text
    assert "uuid-2" in caplog.text


def test_check_for_errors_surfaces_reason_and_auto_schema_hint(uploader: WeaviateUploader):
    """The raised error includes the Weaviate failure reason, and when auto_schema is on
    it points at AUTOSCHEMA_ENABLED as the likely cause (the cluster refusing to
    auto-create the collection)."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", auto_schema=True)
    mock_client = MagicMock()
    mock_client.batch.failed_objects = [
        MagicMock(original_uuid="uuid-1", message='class "MyColl" not found in schema'),
    ]

    with pytest.raises(WriteError) as excinfo:
        uploader.check_for_errors(client=mock_client)
    assert "not found in schema" in str(excinfo.value)
    assert "AUTOSCHEMA_ENABLED" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Uploader — run_data integration
# ---------------------------------------------------------------------------


def _make_run_data_client() -> MagicMock:
    """Weaviate client mock fully wired for run_data:
    - delete_by_record_id loop terminates on first iteration
    - batch.dynamic() yields a batch_client (MagicMock context manager)
    - check_for_errors sees no failures
    """
    mock_client = MagicMock()
    mock_client.collections.get.return_value.data.delete_many.return_value = _delete_resp(
        failed=0, successful=0
    )
    mock_client.batch.failed_objects = []
    return mock_client


def _batch_client_from(mock_client: MagicMock) -> MagicMock:
    """The BatchClient yielded by `with client.batch.dynamic() as batch:`."""
    return mock_client.batch.dynamic.return_value.__enter__.return_value


def test_run_data_raises_when_no_collection_set(uploader: WeaviateUploader, file_data: FileData):
    uploader.upload_config = WeaviateUploaderConfig(collection=None)
    with pytest.raises(ValueError, match="No collection specified"):
        uploader.run_data(data=[{"text": "x"}], file_data=file_data)


def test_run_data_default_mode_passes_unmodified_properties(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
    file_data: FileData,
    monkeypatch: pytest.MonkeyPatch,
):
    """In default mode, properties go to add_object exactly as the stager
    produced them; only embeddings are popped into the vector arg."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=False)
    mock_client = _make_run_data_client()
    connection_config.set_mock_client(mock_client)
    monkeypatch.setattr(uploader, "delete_by_record_id", MagicMock())

    uploader.run_data(
        data=[
            {
                "text": "x",
                "metadata": {"filename": "y"},
                "embeddings": [1.0, 2.0],
            }
        ],
        file_data=file_data,
    )

    batch_client = _batch_client_from(mock_client)
    batch_client.add_object.assert_called_once_with(
        collection="MyColl",
        properties={"text": "x", "metadata": {"filename": "y"}},
        vector=[1.0, 2.0],
    )


def test_run_data_default_mode_does_not_fetch_schema(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
    file_data: FileData,
    monkeypatch: pytest.MonkeyPatch,
):
    """Default mode never reads the destination schema — no introspection
    is performed because conform_to_schema isn't applied."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=False)
    mock_client = _make_run_data_client()
    connection_config.set_mock_client(mock_client)
    monkeypatch.setattr(uploader, "delete_by_record_id", MagicMock())

    uploader.run_data(data=[{"text": "x", "embeddings": [0.1]}], file_data=file_data)

    mock_client.collections.get.return_value.config.get.assert_not_called()


def test_run_data_flatten_mode_applies_schema_conform(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
    file_data: FileData,
    monkeypatch: pytest.MonkeyPatch,
):
    """Flatten mode runs each element through conform_to_schema:
    unknown keys are dropped, missing schema keys are filled with None."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", flatten_metadata=True)
    mock_client = _make_run_data_client()
    config_mock = MagicMock()
    config_mock.properties = [
        _make_property("record_id"),
        _make_property("text"),
        _make_property("filename"),
        _make_property("languages"),
    ]
    mock_client.collections.get.return_value.config.get.return_value = config_mock
    connection_config.set_mock_client(mock_client)
    monkeypatch.setattr(uploader, "delete_by_record_id", MagicMock())

    uploader.run_data(
        data=[
            {
                "record_id": "r1",
                "text": "x",
                "filename": "y",
                "junk_field": "drop me",
                "embeddings": [1.0, 2.0, 3.0],
            }
        ],
        file_data=file_data,
    )

    batch_client = _batch_client_from(mock_client)
    batch_client.add_object.assert_called_once_with(
        collection="MyColl",
        properties={
            "record_id": "r1",
            "text": "x",
            "filename": "y",
            "languages": None,  # missing → null
        },
        vector=[1.0, 2.0, 3.0],
    )


def test_run_data_pops_embeddings_into_vector_arg_and_removes_from_properties(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
    file_data: FileData,
    monkeypatch: pytest.MonkeyPatch,
):
    """Embeddings are always popped from properties so they go to add_object's
    vector= argument, never as a regular property column."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl")
    mock_client = _make_run_data_client()
    connection_config.set_mock_client(mock_client)
    monkeypatch.setattr(uploader, "delete_by_record_id", MagicMock())

    uploader.run_data(
        data=[{"text": "x", "embeddings": [0.1, 0.2, 0.3]}],
        file_data=file_data,
    )

    batch_client = _batch_client_from(mock_client)
    call = batch_client.add_object.call_args
    assert call.kwargs["vector"] == [0.1, 0.2, 0.3]
    assert "embeddings" not in call.kwargs["properties"]


def test_run_data_calls_delete_by_record_id_once_per_invocation(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
    file_data: FileData,
    monkeypatch: pytest.MonkeyPatch,
):
    """Re-runs dedupe by deleting prior writes for this record_id before
    the new batch lands."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl")
    mock_client = _make_run_data_client()
    connection_config.set_mock_client(mock_client)
    delete_spy = MagicMock()
    monkeypatch.setattr(uploader, "delete_by_record_id", delete_spy)

    uploader.run_data(
        data=[{"text": "a"}, {"text": "b"}, {"text": "c"}],
        file_data=file_data,
    )

    delete_spy.assert_called_once()
    # Same client instance is passed both to delete and to add_object
    assert delete_spy.call_args.kwargs["client"] is mock_client


def test_run_data_propagates_check_for_errors_failure(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
    file_data: FileData,
    monkeypatch: pytest.MonkeyPatch,
):
    """If the batch produced any failed_objects, run_data raises WriteError
    after the batch context manager closes."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl")
    mock_client = _make_run_data_client()
    mock_client.batch.failed_objects = [MagicMock(original_uuid="uuid-1", message="rejected")]
    connection_config.set_mock_client(mock_client)
    monkeypatch.setattr(uploader, "delete_by_record_id", MagicMock())

    with pytest.raises(WriteError, match="Failed to upload to weaviate"):
        uploader.run_data(data=[{"text": "x"}], file_data=file_data)


# ---------------------------------------------------------------------------
# WeaviateUploaderConfig — batch mode validation
# ---------------------------------------------------------------------------


def test_uploader_config_requires_at_least_one_batch_mode():
    """All three batch flags off → init raises."""
    with pytest.raises(ValueError, match="No batch mode enabled"):
        WeaviateUploaderConfig(
            collection="MyColl",
            dynamic_batch=False,
            batch_size=None,
            requests_per_minute=None,
        )


def test_uploader_config_rejects_multiple_batch_modes():
    """Setting two batch modes simultaneously is a config error."""
    with pytest.raises(ValueError, match="Multiple batch modes enabled"):
        WeaviateUploaderConfig(
            collection="MyColl",
            dynamic_batch=False,
            batch_size=10,
            requests_per_minute=60,
        )


def test_uploader_config_dynamic_default_alone_is_valid():
    """The default config (dynamic_batch=True, sizes=None) passes validation."""
    cfg = WeaviateUploaderConfig(collection="MyColl")
    assert cfg.dynamic_batch is True
    assert cfg.batch_size is None
    assert cfg.requests_per_minute is None


# ---------------------------------------------------------------------------
# Uploader — auto_schema behavior
# ---------------------------------------------------------------------------


def test_run_data_auto_schema_passes_unknown_keys_without_dropping(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
    file_data: FileData,
    monkeypatch: pytest.MonkeyPatch,
):
    """auto_schema mode uploads every property as-is even when they are not in
    any existing schema — nothing is dropped and the schema is never fetched."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", auto_schema=True)
    mock_client = _make_run_data_client()
    mock_client.collections.exists.return_value = True
    connection_config.set_mock_client(mock_client)
    monkeypatch.setattr(uploader, "delete_by_record_id", MagicMock())

    uploader.run_data(
        data=[{"text": "x", "brand_new_field": "keep me", "embeddings": [0.1]}],
        file_data=file_data,
    )

    batch_client = _batch_client_from(mock_client)
    batch_client.add_object.assert_called_once_with(
        collection="MyColl",
        properties={"text": "x", "brand_new_field": "keep me"},
        vector=[0.1],
    )
    # Schema is never read in auto_schema mode.
    mock_client.collections.get.return_value.config.get.assert_not_called()


def test_run_data_auto_schema_skips_delete_when_collection_absent(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
    file_data: FileData,
    monkeypatch: pytest.MonkeyPatch,
):
    """On the first run in auto_schema mode the collection may not exist yet
    (Weaviate creates it on insert), so delete_by_record_id is skipped rather
    than erroring on a missing collection — but the object is still uploaded."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", auto_schema=True)
    mock_client = _make_run_data_client()
    mock_client.collections.exists.return_value = False
    connection_config.set_mock_client(mock_client)
    delete_spy = MagicMock()
    monkeypatch.setattr(uploader, "delete_by_record_id", delete_spy)

    uploader.run_data(data=[{"text": "x"}], file_data=file_data)

    delete_spy.assert_not_called()
    _batch_client_from(mock_client).add_object.assert_called_once()


def test_run_data_auto_schema_deletes_when_collection_present(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
    file_data: FileData,
    monkeypatch: pytest.MonkeyPatch,
):
    """When the collection already exists, auto_schema mode still purges prior
    records for this record_id before writing the new batch."""
    uploader.upload_config = WeaviateUploaderConfig(collection="MyColl", auto_schema=True)
    mock_client = _make_run_data_client()
    mock_client.collections.exists.return_value = True
    connection_config.set_mock_client(mock_client)
    delete_spy = MagicMock()
    monkeypatch.setattr(uploader, "delete_by_record_id", delete_spy)

    uploader.run_data(data=[{"text": "x"}], file_data=file_data)

    delete_spy.assert_called_once()


def test_uploader_config_auto_schema_defaults_false():
    """auto_schema defaults to False so the connector keeps its pre-existing
    behavior (conform-to-schema in flatten mode, no conform in non-flatten);
    dynamic auto-schema is opt-in."""
    assert WeaviateUploaderConfig(collection="MyColl").auto_schema is False


def test_run_data_default_mode_does_not_conform(
    uploader: WeaviateUploader,
    connection_config: WeaviateConnectionConfigTest,
    file_data: FileData,
    monkeypatch: pytest.MonkeyPatch,
):
    """Non-flatten + auto_schema disabled (the default) uploads properties as-is —
    conform_to_schema is only applied in flatten mode, matching the connector's
    pre-auto_schema behavior (type/element_id are never dropped)."""
    uploader.upload_config = WeaviateUploaderConfig(
        collection="MyColl", auto_schema=False, flatten_metadata=False
    )
    mock_client = _make_run_data_client()
    connection_config.set_mock_client(mock_client)
    monkeypatch.setattr(uploader, "delete_by_record_id", MagicMock())

    uploader.run_data(
        data=[{"type": "Title", "element_id": "e1", "text": "x", "embeddings": [0.1]}],
        file_data=file_data,
    )

    batch_client = _batch_client_from(mock_client)
    batch_client.add_object.assert_called_once_with(
        collection="MyColl",
        properties={"type": "Title", "element_id": "e1", "text": "x"},
        vector=[0.1],
    )
    # No schema is fetched in non-flatten mode, regardless of auto_schema.
    mock_client.collections.get.return_value.config.get.assert_not_called()
