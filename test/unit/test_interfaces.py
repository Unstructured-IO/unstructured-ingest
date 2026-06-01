import json
from pathlib import Path

import pytest
from pydantic import Secret, ValidationError

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.utils import ndjson


def test_failing_connection_config():
    class MyAccessConfig(AccessConfig):
        sensitive_value: str

    class MyConnectionConfig(ConnectionConfig):
        access_config: MyAccessConfig

    with pytest.raises(ValidationError):
        MyConnectionConfig(access_config=MyAccessConfig(sensitive_value="this"))


def test_happy_path_connection_config():
    class MyAccessConfig(AccessConfig):
        sensitive_value: str

    class MyConnectionConfig(ConnectionConfig):
        access_config: Secret[MyAccessConfig]

    connection_config = MyConnectionConfig(access_config=MyAccessConfig(sensitive_value="this"))
    assert connection_config


class _PassthroughStager(UploadStager):
    """Concrete UploadStager that relies entirely on base class defaults."""


class _EvenOnlyStager(UploadStager):
    """Concrete UploadStager that filters out elements with odd ids."""

    def should_include(self, element_dict: dict) -> bool:
        return element_dict.get("id", 0) % 2 == 0


def _make_file_data() -> FileData:
    return FileData(
        connector_type="test",
        identifier="test_id",
        source_identifiers=SourceIdentifiers(filename="test.json", fullpath="test.json"),
    )


def _sample_elements() -> list[dict]:
    return [{"id": i, "text": f"element {i}"} for i in range(6)]


def test_should_include_default_returns_true():
    stager = _PassthroughStager(upload_stager_config=UploadStagerConfig())
    for element in _sample_elements():
        assert stager.should_include(element_dict=element) is True


def test_should_include_default_run_keeps_all_elements_json(tmp_path: Path):
    """Default behavior must not filter anything (regression guard)."""
    input_file = tmp_path / "input.json"
    elements = _sample_elements()
    input_file.write_text(json.dumps(elements))

    stager = _PassthroughStager(upload_stager_config=UploadStagerConfig())
    output_path = stager.run(
        elements_filepath=input_file,
        file_data=_make_file_data(),
        output_dir=tmp_path / "out",
        output_filename="input.json",
    )

    staged = json.loads(output_path.read_text())
    assert len(staged) == len(elements)
    assert [e["id"] for e in staged] == [e["id"] for e in elements]


def test_should_include_default_run_keeps_all_elements_ndjson(tmp_path: Path):
    input_file = tmp_path / "input.ndjson"
    elements = _sample_elements()
    with input_file.open("w") as f:
        ndjson.dump(elements, f)

    stager = _PassthroughStager(upload_stager_config=UploadStagerConfig())
    output_path = stager.run(
        elements_filepath=input_file,
        file_data=_make_file_data(),
        output_dir=tmp_path / "out",
        output_filename="input.ndjson",
    )

    with output_path.open() as f:
        staged = ndjson.load(f)
    assert [e["id"] for e in staged] == [e["id"] for e in elements]


def test_should_include_filters_in_process_whole(tmp_path: Path):
    """JSON input path must consult should_include and drop excluded elements."""
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(_sample_elements()))

    stager = _EvenOnlyStager(upload_stager_config=UploadStagerConfig())
    output_path = stager.run(
        elements_filepath=input_file,
        file_data=_make_file_data(),
        output_dir=tmp_path / "out",
        output_filename="input.json",
    )

    staged = json.loads(output_path.read_text())
    assert [e["id"] for e in staged] == [0, 2, 4]


def test_should_include_filters_in_stream_update(tmp_path: Path):
    """NDJSON input path must consult should_include and drop excluded elements."""
    input_file = tmp_path / "input.ndjson"
    with input_file.open("w") as f:
        ndjson.dump(_sample_elements(), f)

    stager = _EvenOnlyStager(upload_stager_config=UploadStagerConfig())
    output_path = stager.run(
        elements_filepath=input_file,
        file_data=_make_file_data(),
        output_dir=tmp_path / "out",
        output_filename="input.ndjson",
    )

    with output_path.open() as f:
        staged = ndjson.load(f)
    assert [e["id"] for e in staged] == [0, 2, 4]


def test_should_include_filter_can_drop_all_elements(tmp_path: Path):
    """If should_include rejects everything, the staged file is empty but valid."""

    class _RejectAllStager(UploadStager):
        def should_include(self, element_dict: dict) -> bool:
            return False

    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(_sample_elements()))

    stager = _RejectAllStager(upload_stager_config=UploadStagerConfig())
    output_path = stager.run(
        elements_filepath=input_file,
        file_data=_make_file_data(),
        output_dir=tmp_path / "out",
        output_filename="input.json",
    )

    assert json.loads(output_path.read_text()) == []


def test_should_include_runs_before_conform_dict(tmp_path: Path):
    """Excluded elements must not be passed to conform_dict (no wasted work / side effects)."""

    seen_by_conform: list[int] = []

    class _TrackingStager(UploadStager):
        def should_include(self, element_dict: dict) -> bool:
            return element_dict.get("id", 0) >= 3

        def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
            seen_by_conform.append(element_dict["id"])
            return element_dict

    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(_sample_elements()))

    stager = _TrackingStager(upload_stager_config=UploadStagerConfig())
    stager.run(
        elements_filepath=input_file,
        file_data=_make_file_data(),
        output_dir=tmp_path / "out",
        output_filename="input.json",
    )

    assert seen_by_conform == [3, 4, 5]
