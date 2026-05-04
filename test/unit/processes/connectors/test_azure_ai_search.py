import logging
from unittest.mock import MagicMock

import pytest
from azure.search.documents.indexes.models import (
    ComplexField,
    SearchFieldDataType,
    SearchIndex,
    SimpleField,
)

from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.azure_ai_search import (
    AzureAISearchUploader,
    AzureAISearchUploaderConfig,
)


def make_field(name: str, key: bool = False, filterable: bool = False, sub_fields=None):
    """Build a mock SearchField. ``sub_fields`` mirrors Azure's ComplexField semantics:
    None for simple fields, a list (possibly empty) for complex/composite fields."""
    f = MagicMock()
    f.name = name
    f.key = key
    f.filterable = filterable
    f.fields = sub_fields
    return f


def make_index(field_specs: list[tuple[str, bool, bool]]):
    """Build a mock index with fields defined by (name, key, filterable) tuples."""
    fields = [
        make_field(name, key=key, filterable=filterable) for name, key, filterable in field_specs
    ]
    index = MagicMock()
    index.fields = fields
    return index


def make_uploader(**kwargs) -> AzureAISearchUploader:
    connection_config = MagicMock()
    config = AzureAISearchUploaderConfig(**kwargs)
    return AzureAISearchUploader(
        upload_config=config,
        connection_config=connection_config,
    )


# ---------------------------------------------------------------------------
# get_index_schema
# ---------------------------------------------------------------------------


def test_get_index_schema_returns_flat_dict_for_simple_fields():
    uploader = make_uploader()
    index = make_index([("id", True, False), ("text", False, False), ("record_id", False, True)])
    assert uploader.get_index_schema(index) == {"id": None, "text": None, "record_id": None}


def test_get_index_schema_empty_index():
    uploader = make_uploader()
    index = make_index([])
    assert uploader.get_index_schema(index) == {}


def test_get_index_schema_walks_into_complex_field_subfields():
    uploader = make_uploader()
    metadata_subfields = [make_field("filename"), make_field("filetype")]
    index = MagicMock()
    index.fields = [
        make_field("id", key=True),
        make_field("metadata", sub_fields=metadata_subfields),
    ]
    assert uploader.get_index_schema(index) == {
        "id": None,
        "metadata": {"filename": None, "filetype": None},
    }


def test_get_index_schema_handles_nested_complex_fields():
    uploader = make_uploader()
    data_source = make_field("data_source", sub_fields=[make_field("url"), make_field("version")])
    metadata = make_field("metadata", sub_fields=[make_field("filename"), data_source])
    index = MagicMock()
    index.fields = [make_field("id", key=True), metadata]
    assert uploader.get_index_schema(index) == {
        "id": None,
        "metadata": {
            "filename": None,
            "data_source": {"url": None, "version": None},
        },
    }


def test_get_index_schema_empty_complex_field_yields_empty_dict():
    """An empty ComplexField means no sub-fields are allowed — different from a simple field."""
    uploader = make_uploader()
    empty_complex = make_field("empty", sub_fields=[])
    index = MagicMock()
    index.fields = [empty_complex]
    assert uploader.get_index_schema(index) == {"empty": {}}


# ---------------------------------------------------------------------------
# can_delete
# ---------------------------------------------------------------------------


def test_can_delete_true_when_record_id_field_is_filterable():
    uploader = make_uploader()
    index = make_index([("id", True, False), ("record_id", False, True)])
    assert uploader.can_delete(index) is True


def test_can_delete_false_when_record_id_field_not_filterable():
    uploader = make_uploader()
    index = make_index([("id", True, False), ("record_id", False, False)])
    assert uploader.can_delete(index) is False


def test_can_delete_false_when_record_id_field_missing():
    uploader = make_uploader()
    index = make_index([("id", True, False)])
    assert uploader.can_delete(index) is False


# ---------------------------------------------------------------------------
# get_index_key
# ---------------------------------------------------------------------------


def test_get_index_key_returns_key_field_name():
    uploader = make_uploader()
    index = make_index([("id", True, False), ("text", False, False)])
    assert uploader.get_index_key(index) == "id"


def test_get_index_key_raises_when_no_key_field():
    uploader = make_uploader()
    index = make_index([("text", False, False)])
    with pytest.raises(IngestValueError):
        uploader.get_index_key(index)


# ---------------------------------------------------------------------------
# filter_doc
# ---------------------------------------------------------------------------


def test_filter_doc_removes_unknown_top_level_fields():
    uploader = make_uploader()
    doc = {"id": "1", "text": "hello", "unknown_field": "drop me"}
    result = uploader.filter_doc(doc, {"id": None, "text": None})
    assert result == {"id": "1", "text": "hello"}
    assert "unknown_field" not in result


def test_filter_doc_keeps_all_fields_when_all_known():
    uploader = make_uploader()
    doc = {"id": "1", "text": "hello"}
    result = uploader.filter_doc(doc, {"id": None, "text": None, "extra": None})
    assert result == {"id": "1", "text": "hello"}


def test_filter_doc_empty_doc():
    uploader = make_uploader()
    assert uploader.filter_doc({}, {"id": None, "text": None}) == {}


def test_filter_doc_drops_unknown_field_inside_complex_metadata():
    """Reproduces the Azure 400 'table_extraction_method does not exist on type
    search.complex.metadata' bug: nested fields not present in the complex sub-schema
    must be dropped before upload."""
    uploader = make_uploader()
    schema = {
        "id": None,
        "metadata": {"filename": None, "filetype": None},
    }
    doc = {
        "id": "1",
        "metadata": {
            "filename": "doc.pdf",
            "filetype": "pdf",
            "table_extraction_method": "auto",
        },
    }
    result = uploader.filter_doc(doc, schema)
    assert result == {
        "id": "1",
        "metadata": {"filename": "doc.pdf", "filetype": "pdf"},
    }


def test_filter_doc_recurses_into_doubly_nested_complex_fields():
    uploader = make_uploader()
    schema = {
        "metadata": {
            "filename": None,
            "data_source": {"url": None, "version": None},
        },
    }
    doc = {
        "metadata": {
            "filename": "doc.pdf",
            "extra_meta": "drop",
            "data_source": {
                "url": "x",
                "version": "1",
                "secret_field": "drop",
            },
        },
    }
    result = uploader.filter_doc(doc, schema)
    assert result == {
        "metadata": {
            "filename": "doc.pdf",
            "data_source": {"url": "x", "version": "1"},
        },
    }


def test_filter_doc_filters_within_collection_of_complex_objects():
    uploader = make_uploader()
    schema = {"items": {"name": None}}
    doc = {"items": [{"name": "a", "drop": "x"}, {"name": "b"}]}
    result = uploader.filter_doc(doc, schema)
    assert result == {"items": [{"name": "a"}, {"name": "b"}]}


def test_filter_doc_passes_through_collection_of_simple_values():
    uploader = make_uploader()
    schema = {"tags": None}
    doc = {"tags": ["a", "b", "c"]}
    result = uploader.filter_doc(doc, schema)
    assert result == {"tags": ["a", "b", "c"]}


def test_filter_doc_does_not_mutate_input_doc():
    uploader = make_uploader()
    schema = {"id": None, "metadata": {"filename": None}}
    doc = {
        "id": "1",
        "metadata": {"filename": "doc.pdf", "table_extraction_method": "auto"},
    }
    uploader.filter_doc(doc, schema)
    assert doc == {
        "id": "1",
        "metadata": {"filename": "doc.pdf", "table_extraction_method": "auto"},
    }


# ---------------------------------------------------------------------------
# run_data: dropped fields are logged
# ---------------------------------------------------------------------------


def test_run_data_logs_dropped_fields(caplog):
    uploader = make_uploader()

    index = make_index([("id", True, False), ("record_id", False, True), ("text", False, False)])
    uploader.get_index = MagicMock(return_value=index)
    uploader.can_delete = MagicMock(return_value=False)

    file_data = MagicMock()
    data = [{"id": "1", "text": "hello", "extra_field": "drop me"}]

    search_client_ctx = MagicMock()
    search_client_ctx.__enter__ = MagicMock(return_value=MagicMock())
    search_client_ctx.__exit__ = MagicMock(return_value=False)
    uploader.connection_config.get_search_client.return_value = search_client_ctx
    uploader.write_dict = MagicMock()

    with caplog.at_level(logging.INFO):
        uploader.run_data(data=data, file_data=file_data)

    assert any("extra_field" in record.message for record in caplog.records)


def test_run_data_no_log_when_no_dropped_fields(caplog):
    uploader = make_uploader()

    index = make_index([("id", True, False), ("record_id", False, True), ("text", False, False)])
    uploader.get_index = MagicMock(return_value=index)
    uploader.can_delete = MagicMock(return_value=False)

    file_data = MagicMock()
    data = [{"id": "1", "text": "hello"}]

    search_client_ctx = MagicMock()
    search_client_ctx.__enter__ = MagicMock(return_value=MagicMock())
    search_client_ctx.__exit__ = MagicMock(return_value=False)
    uploader.connection_config.get_search_client.return_value = search_client_ctx
    uploader.write_dict = MagicMock()

    with caplog.at_level(logging.INFO):
        uploader.run_data(data=data, file_data=file_data)

    assert not any("will be dropped" in record.message for record in caplog.records)


# ---------------------------------------------------------------------------
# Integration with the real Azure SDK schema objects (no API calls)
# ---------------------------------------------------------------------------


def _build_realistic_index() -> SearchIndex:
    """Mirrors the shape of the production index that triggered the
    'table_extraction_method does not exist on type search.complex.metadata' bug."""
    data_source = ComplexField(
        name="data_source",
        fields=[
            SimpleField(name="url", type=SearchFieldDataType.String),
            SimpleField(name="version", type=SearchFieldDataType.String),
        ],
    )
    metadata = ComplexField(
        name="metadata",
        fields=[
            SimpleField(name="filename", type=SearchFieldDataType.String),
            SimpleField(name="filetype", type=SearchFieldDataType.String),
            data_source,
        ],
    )
    return SearchIndex(
        name="test-index",
        fields=[
            SimpleField(name="id", type=SearchFieldDataType.String, key=True),
            SimpleField(name="text", type=SearchFieldDataType.String),
            metadata,
        ],
    )


def test_get_index_schema_against_real_azure_sdk_objects():
    uploader = make_uploader()
    schema = uploader.get_index_schema(_build_realistic_index())
    assert schema == {
        "id": None,
        "text": None,
        "metadata": {
            "filename": None,
            "filetype": None,
            "data_source": {"url": None, "version": None},
        },
    }


def test_filter_doc_drops_table_extraction_method_against_real_sdk_schema():
    """End-to-end: the exact scenario from the production 400 error — a document with
    metadata.table_extraction_method against a complex metadata schema that doesn't
    declare it. Must be dropped before upload."""
    uploader = make_uploader()
    schema = uploader.get_index_schema(_build_realistic_index())
    doc = {
        "id": "abc",
        "text": "some content",
        "metadata": {
            "filename": "doc.pdf",
            "filetype": "pdf",
            "table_extraction_method": "auto",
            "data_source": {
                "url": "https://example.com/doc.pdf",
                "version": "1",
                "etag": "deadbeef",
            },
        },
        "stray_top_level": "should be dropped too",
    }
    filtered = uploader.filter_doc(doc, schema)
    assert filtered == {
        "id": "abc",
        "text": "some content",
        "metadata": {
            "filename": "doc.pdf",
            "filetype": "pdf",
            "data_source": {
                "url": "https://example.com/doc.pdf",
                "version": "1",
            },
        },
    }
