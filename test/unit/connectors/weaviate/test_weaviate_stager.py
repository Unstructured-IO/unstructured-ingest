import time

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.weaviate.weaviate import (
    WeaviateUploadStager,
    WeaviateUploadStagerConfig,
)


@pytest.fixture
def non_utc_timezone(monkeypatch):
    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset is unavailable")
    with monkeypatch.context() as patch:
        patch.setenv("TZ", "EST5EDT")
        time.tzset()
        try:
            yield
        finally:
            patch.undo()
            time.tzset()


@pytest.mark.parametrize("flatten_metadata", [False, True])
@pytest.mark.parametrize(
    "value, expected",
    [
        ("1700000000", "2023-11-14T22:13:20.000000Z"),
        ("1700000000.25", "2023-11-14T22:13:20.250000Z"),
        ("2023-11-15T03:43:20+05:30", "2023-11-14T22:13:20.000000Z"),
        ("2023-11-14T17:13:20-05:00", "2023-11-14T22:13:20.000000Z"),
        ("2023-11-14T22:13:20Z", "2023-11-14T22:13:20.000000Z"),
        ("2023-11-14T22:13:20", "2023-11-14T22:13:20.000000Z"),
    ],
)
def test_staged_dates_are_utc(non_utc_timezone, flatten_metadata, value, expected):
    stager = WeaviateUploadStager(
        upload_stager_config=WeaviateUploadStagerConfig(
            flatten_metadata=flatten_metadata, auto_schema=True
        )
    )
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath="x.txt", filename="x.txt"),
        connector_type="weaviate",
        identifier="rec-123",
    )
    source_dates = dict.fromkeys(("date_created", "date_modified", "date_processed"), value)
    result = stager.conform_dict(
        {"metadata": {"data_source": source_dates, "last_modified": value}}, file_data
    )

    if flatten_metadata:
        for field in source_dates:
            assert result[f"data_source_{field}"] == expected
        assert result["last_modified"] == expected
    else:
        for field in source_dates:
            assert result["metadata"]["data_source"][field] == expected
        assert result["metadata"]["last_modified"] == expected
