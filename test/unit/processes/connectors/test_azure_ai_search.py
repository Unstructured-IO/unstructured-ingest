import logging
from unittest.mock import MagicMock

import pytest

from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors import azure_ai_search as aas_module
from unstructured_ingest.processes.connectors.azure_ai_search import (
    AzureAISearchUploader,
    AzureAISearchUploaderConfig,
)


def make_field(name: str, *, sub_fields=None, key: bool = False, filterable: bool = False):
    """Build a mock ``SearchField``; ``sub_fields=None`` for primitives,
    a list of mock fields for ComplexType."""
    f = MagicMock()
    f.name = name
    f.fields = sub_fields
    f.key = key
    f.filterable = filterable
    return f


def make_index(field_specs: list[tuple[str, bool, bool]]):
    """Build a mock index with top-level-only fields defined by (name, key, filterable)."""
    fields = [
        make_field(name, key=key, filterable=filterable) for name, key, filterable in field_specs
    ]
    index = MagicMock()
    index.fields = fields
    return index


def make_nested_index(fields_spec):
    """Build a mock index from ``[{"name", "fields"?, "key"?, "filterable"?}, ...]``;
    ``fields`` is a child spec list for complex types or absent/``None`` for primitives."""

    def build(spec):
        out = []
        for entry in spec:
            sub = entry.get("fields")
            sub_fields = build(sub) if sub is not None else None
            out.append(
                make_field(
                    entry["name"],
                    sub_fields=sub_fields,
                    key=entry.get("key", False),
                    filterable=entry.get("filterable", False),
                )
            )
        return out

    index = MagicMock()
    index.fields = build(fields_spec)
    return index


def canonical_schema_index():
    """Mock of the canonical Azure AI Search schema used by this connector (mirrors the
    integration-test fixture, with nested ``metadata.coordinates`` and ``metadata.data_source``)."""
    return make_nested_index(
        [
            {"name": "id", "key": True},
            {"name": "record_id", "filterable": True},
            {"name": "element_id"},
            {"name": "text"},
            {"name": "type"},
            {
                "name": "metadata",
                "fields": [
                    {"name": "filename"},
                    {"name": "filetype"},
                    {"name": "last_modified"},
                    {"name": "languages"},
                    {"name": "page_number"},
                    {"name": "links"},
                    {"name": "regex_metadata"},
                    {"name": "orig_elements"},
                    {
                        "name": "coordinates",
                        "fields": [
                            {"name": "system"},
                            {"name": "layout_width"},
                            {"name": "layout_height"},
                            {"name": "points"},
                        ],
                    },
                    {
                        "name": "data_source",
                        "fields": [
                            {"name": "url"},
                            {"name": "version"},
                            {"name": "date_created"},
                            {"name": "date_modified"},
                            {"name": "date_processed"},
                            {"name": "permissions_data"},
                            {"name": "record_locator"},
                        ],
                    },
                ],
            },
            {"name": "embeddings"},
        ]
    )


def make_uploader(**kwargs) -> AzureAISearchUploader:
    connection_config = MagicMock()
    config = AzureAISearchUploaderConfig(**kwargs)
    return AzureAISearchUploader(
        upload_config=config,
        connection_config=connection_config,
    )


def test_get_index_field_names_returns_set_of_names():
    uploader = make_uploader()
    index = make_index([("id", True, False), ("text", False, False), ("record_id", False, True)])
    assert uploader.get_index_field_names(index) == {"id", "text", "record_id"}


def test_get_index_field_names_empty_index():
    uploader = make_uploader()
    index = make_index([])
    assert uploader.get_index_field_names(index) == set()


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


def test_get_index_key_returns_key_field_name():
    uploader = make_uploader()
    index = make_index([("id", True, False), ("text", False, False)])
    assert uploader.get_index_key(index) == "id"


def test_get_index_key_raises_when_no_key_field():
    uploader = make_uploader()
    index = make_index([("text", False, False)])
    with pytest.raises(IngestValueError):
        uploader.get_index_key(index)


def test_filter_doc_removes_unknown_fields():
    uploader = make_uploader()
    doc = {"id": "1", "text": "hello", "unknown_field": "drop me"}
    result = uploader.filter_doc(doc, {"id", "text"})
    assert result == {"id": "1", "text": "hello"}
    assert "unknown_field" not in result


def test_filter_doc_keeps_all_fields_when_all_known():
    uploader = make_uploader()
    doc = {"id": "1", "text": "hello"}
    result = uploader.filter_doc(doc, {"id", "text", "extra"})
    assert result == {"id": "1", "text": "hello"}


def test_filter_doc_empty_doc():
    uploader = make_uploader()
    assert uploader.filter_doc({}, {"id", "text"}) == {}


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


def test_get_index_field_tree_top_level_only_marks_all_leaves():
    uploader = make_uploader()
    index = make_nested_index(
        [{"name": "id"}, {"name": "text"}, {"name": "record_id"}, {"name": "embeddings"}]
    )
    assert uploader.get_index_field_tree(index) == {
        "id": None,
        "text": None,
        "record_id": None,
        "embeddings": None,
    }


def test_get_index_field_tree_canonical_schema_mirrors_nesting():
    uploader = make_uploader()
    tree = uploader.get_index_field_tree(canonical_schema_index())

    assert set(tree.keys()) == {
        "id",
        "record_id",
        "element_id",
        "text",
        "type",
        "metadata",
        "embeddings",
    }
    assert tree["id"] is None
    assert tree["embeddings"] is None
    assert isinstance(tree["metadata"], dict)
    assert tree["metadata"]["filename"] is None
    assert tree["metadata"]["languages"] is None
    assert isinstance(tree["metadata"]["coordinates"], dict)
    assert tree["metadata"]["coordinates"]["points"] is None
    assert isinstance(tree["metadata"]["data_source"], dict)
    assert tree["metadata"]["data_source"]["record_locator"] is None
    assert tree["metadata"]["data_source"]["url"] is None


def test_get_index_field_tree_collection_of_complex_treated_as_complex():
    """Collection(ComplexType) populates ``.fields`` like ComplexType;
    the tree builder treats both identically."""
    uploader = make_uploader()
    index = make_nested_index(
        [
            {"name": "id"},
            {
                "name": "key_value_pairs",
                "fields": [{"name": "key"}, {"name": "value"}, {"name": "confidence"}],
            },
        ]
    )
    tree = uploader.get_index_field_tree(index)
    assert tree["id"] is None
    assert tree["key_value_pairs"] == {"key": None, "value": None, "confidence": None}


def test_get_index_field_tree_empty_index():
    uploader = make_uploader()
    index = make_nested_index([])
    assert uploader.get_index_field_tree(index) == {}


def test_get_index_field_tree_caps_at_max_depth(monkeypatch):
    """Defensive cap: depth ``>= _MAX_INDEX_FIELD_DEPTH`` clips the sub-tree to ``{}``,
    so anything beyond is treated as unknown by the filter."""
    monkeypatch.setattr(aas_module, "_MAX_INDEX_FIELD_DEPTH", 3)
    uploader = make_uploader()
    index = make_nested_index(
        [
            {
                "name": "lvl0",
                "fields": [
                    {
                        "name": "lvl1",
                        "fields": [
                            {
                                "name": "lvl2",
                                "fields": [{"name": "lvl3_should_be_clipped"}],
                            }
                        ],
                    }
                ],
            }
        ]
    )
    tree = uploader.get_index_field_tree(index)
    assert tree == {"lvl0": {"lvl1": {"lvl2": {}}}}


def test_filter_doc_against_tree_no_op_when_doc_matches_schema():
    uploader = make_uploader()
    tree = {"id": None, "text": None, "metadata": {"filename": None, "page_number": None}}
    doc = {"id": "1", "text": "hello", "metadata": {"filename": "f.pdf", "page_number": "1"}}
    assert uploader.filter_doc_against_tree(doc, tree) == doc


def test_filter_doc_against_tree_drops_unknown_top_level_key():
    uploader = make_uploader()
    tree = {"id": None, "text": None}
    doc = {"id": "1", "text": "hello", "extra": "drop me"}
    assert uploader.filter_doc_against_tree(doc, tree) == {"id": "1", "text": "hello"}


def test_filter_doc_against_tree_drops_unknown_nested_key_regression_table_extraction_method():
    """Regression (2026-05-01): new ``unstructured`` field ``metadata.table_extraction_method``
    causes Azure AI Search 400s; the recursive filter must drop it before upload."""
    uploader = make_uploader()
    tree = uploader.get_index_field_tree(canonical_schema_index())
    doc = {
        "id": "abc",
        "record_id": "rec-1",
        "text": "some text",
        "metadata": {
            "filename": "doc.pdf",
            "filetype": "application/pdf",
            "table_extraction_method": "tatr",
        },
    }
    filtered = uploader.filter_doc_against_tree(doc, tree)
    assert "table_extraction_method" not in filtered["metadata"]
    assert filtered["metadata"] == {"filename": "doc.pdf", "filetype": "application/pdf"}
    assert filtered["id"] == "abc"
    assert filtered["record_id"] == "rec-1"
    assert filtered["text"] == "some text"


def test_filter_doc_against_tree_drops_unknown_deeply_nested_key():
    uploader = make_uploader()
    tree = uploader.get_index_field_tree(canonical_schema_index())
    doc = {
        "id": "abc",
        "metadata": {
            "data_source": {
                "url": "s3://bucket/x.pdf",
                "future_undeclared_field": "hello",
            },
        },
    }
    filtered = uploader.filter_doc_against_tree(doc, tree)
    assert filtered["metadata"]["data_source"] == {"url": "s3://bucket/x.pdf"}


def test_filter_doc_against_tree_preserves_string_leaves_modeled_as_edm_string():
    """Stager-stringified sub-trees (e.g. ``record_locator``, ``points``) are modeled as Edm.String
    leaves (``tree[key] is None``) and must pass through unchanged."""
    uploader = make_uploader()
    tree = uploader.get_index_field_tree(canonical_schema_index())
    doc = {
        "metadata": {
            "data_source": {
                "record_locator": '{"protocol": "s3", "remote_file_path": "s3://x"}',
                "permissions_data": '[{"permission": "read"}]',
            },
            "coordinates": {"points": "[[0, 0], [1, 1]]"},
            "regex_metadata": '{"foo": "bar"}',
        }
    }
    filtered = uploader.filter_doc_against_tree(doc, tree)
    assert filtered["metadata"]["data_source"]["record_locator"] == doc["metadata"][
        "data_source"
    ]["record_locator"]
    assert (
        filtered["metadata"]["data_source"]["permissions_data"]
        == doc["metadata"]["data_source"]["permissions_data"]
    )
    assert filtered["metadata"]["coordinates"]["points"] == doc["metadata"]["coordinates"]["points"]
    assert filtered["metadata"]["regex_metadata"] == doc["metadata"]["regex_metadata"]


def test_filter_doc_against_tree_passes_primitive_collection_through():
    uploader = make_uploader()
    tree = uploader.get_index_field_tree(canonical_schema_index())
    doc = {
        "embeddings": [0.1, 0.2, 0.3],
        "metadata": {"languages": ["eng", "fra"], "links": ["{...}", "{...}"]},
    }
    filtered = uploader.filter_doc_against_tree(doc, tree)
    assert filtered["embeddings"] == [0.1, 0.2, 0.3]
    assert filtered["metadata"]["languages"] == ["eng", "fra"]
    assert filtered["metadata"]["links"] == ["{...}", "{...}"]


def test_filter_doc_against_tree_recurses_into_collection_of_complex():
    uploader = make_uploader()
    index = make_nested_index(
        [
            {"name": "id"},
            {
                "name": "key_value_pairs",
                "fields": [{"name": "key"}, {"name": "value"}, {"name": "confidence"}],
            },
        ]
    )
    tree = uploader.get_index_field_tree(index)
    doc = {
        "id": "1",
        "key_value_pairs": [
            {"key": "k1", "value": "v1", "confidence": 0.9, "extra_pair_field": "drop"},
            {"key": "k2", "value": "v2", "confidence": 0.8},
        ],
    }
    filtered = uploader.filter_doc_against_tree(doc, tree)
    assert filtered == {
        "id": "1",
        "key_value_pairs": [
            {"key": "k1", "value": "v1", "confidence": 0.9},
            {"key": "k2", "value": "v2", "confidence": 0.8},
        ],
    }


def test_filter_doc_against_tree_sparse_doc_no_fabrication():
    """Missing optional schema keys should not be invented in the output."""
    uploader = make_uploader()
    tree = uploader.get_index_field_tree(canonical_schema_index())
    doc = {"id": "abc", "metadata": {"filename": "f.pdf"}}
    filtered = uploader.filter_doc_against_tree(doc, tree)
    assert filtered == {"id": "abc", "metadata": {"filename": "f.pdf"}}


def test_filter_doc_against_tree_empty_doc():
    uploader = make_uploader()
    tree = uploader.get_index_field_tree(canonical_schema_index())
    assert uploader.filter_doc_against_tree({}, tree) == {}


def test_filter_doc_against_tree_handles_metadata_modeled_as_edm_string():
    """Customer schemas that model ``metadata`` as Edm.String (JSON blob) yield
    ``tree["metadata"] is None``; the value must pass through verbatim regardless of shape."""
    uploader = make_uploader()
    tree = {"id": None, "metadata": None}
    doc = {"id": "1", "metadata": {"any": "shape", "nested": {"a": 1}}}
    filtered = uploader.filter_doc_against_tree(doc, tree)
    assert filtered == doc


def test_collect_dropped_paths_empty_when_no_drops():
    uploader = make_uploader()
    tree = uploader.get_index_field_tree(canonical_schema_index())
    doc = {"id": "1", "metadata": {"filename": "f.pdf"}}
    assert uploader.collect_dropped_paths(doc, tree) == []


def test_collect_dropped_paths_reports_dotted_nested_path():
    uploader = make_uploader()
    tree = uploader.get_index_field_tree(canonical_schema_index())
    doc = {"id": "1", "metadata": {"filename": "f.pdf", "table_extraction_method": "tatr"}}
    assert uploader.collect_dropped_paths(doc, tree) == ["metadata.table_extraction_method"]


def test_collect_dropped_paths_mixes_top_level_and_nested():
    uploader = make_uploader()
    tree = uploader.get_index_field_tree(canonical_schema_index())
    doc = {
        "id": "1",
        "totally_unknown_top": "x",
        "metadata": {
            "filename": "f.pdf",
            "table_extraction_method": "tatr",
            "data_source": {"url": "s3://x", "future_field": "y"},
        },
    }
    assert uploader.collect_dropped_paths(doc, tree) == [
        "metadata.data_source.future_field",
        "metadata.table_extraction_method",
        "totally_unknown_top",
    ]


def test_collect_dropped_paths_dedup_and_sort_across_collection_items():
    """Repeated unknown sub-keys across list items are reported once, sorted with the rest."""
    uploader = make_uploader()
    index = make_nested_index(
        [
            {"name": "id"},
            {
                "name": "key_value_pairs",
                "fields": [{"name": "key"}, {"name": "value"}],
            },
        ]
    )
    tree = uploader.get_index_field_tree(index)
    doc = {
        "id": "1",
        "key_value_pairs": [
            {"key": "k", "value": "v", "junk": 1},
            {"key": "k2", "value": "v2", "junk": 2},
        ],
    }
    assert uploader.collect_dropped_paths(doc, tree) == ["key_value_pairs.junk"]


def test_run_data_strips_nested_unknown_field_before_upload(caplog):
    """End-to-end: ``run_data`` must log the dropped dotted path and hand ``write_dict``
    a doc with ``metadata.table_extraction_method`` removed."""
    uploader = make_uploader()
    uploader.get_index = MagicMock(return_value=canonical_schema_index())
    uploader.can_delete = MagicMock(return_value=False)
    uploader.write_dict = MagicMock()

    search_client_ctx = MagicMock()
    search_client_ctx.__enter__ = MagicMock(return_value=MagicMock())
    search_client_ctx.__exit__ = MagicMock(return_value=False)
    uploader.connection_config.get_search_client.return_value = search_client_ctx

    file_data = MagicMock()
    data = [
        {
            "id": "abc",
            "record_id": "rec-1",
            "text": "some text",
            "metadata": {
                "filename": "doc.pdf",
                "table_extraction_method": "tatr",
                "data_source": {"url": "s3://x.pdf"},
            },
        }
    ]

    with caplog.at_level(logging.INFO):
        uploader.run_data(data=data, file_data=file_data)

    assert any(
        "metadata.table_extraction_method" in r.message
        and "will be dropped" in r.message
        for r in caplog.records
    ), "expected the dotted path to appear in the dropped-fields INFO log"

    uploader.write_dict.assert_called_once()
    sent_chunk = uploader.write_dict.call_args.kwargs["elements_dict"]
    assert len(sent_chunk) == 1
    sent_doc = sent_chunk[0]
    assert "table_extraction_method" not in sent_doc["metadata"]
    assert sent_doc["metadata"] == {
        "filename": "doc.pdf",
        "data_source": {"url": "s3://x.pdf"},
    }
    assert sent_doc["id"] == "abc"


def test_run_data_no_log_and_passthrough_when_doc_matches_canonical_schema(caplog):
    uploader = make_uploader()
    uploader.get_index = MagicMock(return_value=canonical_schema_index())
    uploader.can_delete = MagicMock(return_value=False)
    uploader.write_dict = MagicMock()

    search_client_ctx = MagicMock()
    search_client_ctx.__enter__ = MagicMock(return_value=MagicMock())
    search_client_ctx.__exit__ = MagicMock(return_value=False)
    uploader.connection_config.get_search_client.return_value = search_client_ctx

    file_data = MagicMock()
    doc = {
        "id": "abc",
        "record_id": "rec-1",
        "text": "some text",
        "metadata": {
            "filename": "doc.pdf",
            "page_number": "1",
            "data_source": {"url": "s3://x.pdf", "version": "1"},
            "coordinates": {"system": "PixelSpace", "points": "[[0,0],[1,1]]"},
        },
        "embeddings": [0.1, 0.2],
    }

    with caplog.at_level(logging.INFO):
        uploader.run_data(data=[doc], file_data=file_data)

    assert not any("will be dropped" in r.message for r in caplog.records)
    sent_chunk = uploader.write_dict.call_args.kwargs["elements_dict"]
    assert list(sent_chunk) == [doc]


def test_legacy_filter_doc_still_returns_top_level_only_filter():
    """The legacy depth-0 ``filter_doc`` is no longer called by ``run_data`` but must keep
    its original semantics for any subclass override or external caller."""
    uploader = make_uploader()
    doc = {"id": "1", "metadata": {"foo": "bar", "table_extraction_method": "tatr"}, "junk": "x"}
    result = uploader.filter_doc(doc, {"id", "metadata"})
    assert result == {"id": "1", "metadata": {"foo": "bar", "table_extraction_method": "tatr"}}
    assert "junk" not in result


def test_legacy_get_index_field_names_still_returns_top_level_set():
    uploader = make_uploader()
    tree_index = canonical_schema_index()
    assert uploader.get_index_field_names(tree_index) == {
        "id",
        "record_id",
        "element_id",
        "text",
        "type",
        "metadata",
        "embeddings",
    }
