import logging
from unittest.mock import MagicMock

import pytest

from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.azure_ai_search import (
    AzureAISearchUploader,
    AzureAISearchUploaderConfig,
)


def make_index(field_specs: list[tuple[str, bool, bool]]):
    """Build a mock index with fields defined by (name, key, filterable) tuples."""
    fields = []
    for name, key, filterable in field_specs:
        f = MagicMock()
        f.name = name
        f.key = key
        f.filterable = filterable
        fields.append(f)
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
# get_index_field_names
# ---------------------------------------------------------------------------


def test_get_index_field_names_returns_set_of_names():
    uploader = make_uploader()
    index = make_index([("id", True, False), ("text", False, False), ("record_id", False, True)])
    assert uploader.get_index_field_names(index) == {"id", "text", "record_id"}


def test_get_index_field_names_empty_index():
    uploader = make_uploader()
    index = make_index([])
    assert uploader.get_index_field_names(index) == set()


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
