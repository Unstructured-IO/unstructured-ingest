"""Tests for the base ``UploadStager`` streaming behavior.

The ``.json`` / ``process_whole`` path used to ``json.load`` the entire element
file, build a full conformed list, and write it whole - holding 3+ copies of a
several-hundred-MB partition output resident and OOM-killing the stager on a
48k-page document. ``process_whole`` now streams element-by-element. These tests
assert:

* output parity with the old behavior (conform + should_include filtering),
* the whole-file ``json.load`` path is no longer taken,
* peak memory stays flat as the input grows.
"""

import json
import tracemalloc
from dataclasses import dataclass
from pathlib import Path

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.interfaces.upload_stager import UploadStager, UploadStagerConfig
from unstructured_ingest.utils import data_prep


class _StagerConfig(UploadStagerConfig):
    pass


@dataclass
class _DemoStager(UploadStager):
    """Adds a conform marker and drops elements flagged ``drop``."""

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        conformed = dict(element_dict)
        conformed["conformed_by"] = file_data.identifier
        return conformed

    def should_include(self, element_dict: dict) -> bool:
        return not element_dict.get("drop", False)


def _file_data() -> FileData:
    return FileData(
        identifier="rec-42",
        connector_type="test",
        source_identifiers=SourceIdentifiers(filename="doc.pdf", fullpath="doc.pdf"),
    )


def _stager() -> _DemoStager:
    return _DemoStager(upload_stager_config=_StagerConfig())


ELEMENTS = [
    {"element_id": "a", "text": "keep me ☃", "metadata": {"page_number": 1}},
    {"element_id": "b", "text": "drop me", "drop": True},
    {"element_id": "c", "text": "keep me too", "metadata": {"page_number": 2}},
]


def test_process_whole_json_conforms_and_filters(tmp_path: Path) -> None:
    input_file = tmp_path / "in.json"
    with input_file.open("w") as f:
        json.dump(ELEMENTS, f, indent=2, ensure_ascii=False)

    output_file = _stager().run(
        elements_filepath=input_file,
        file_data=_file_data(),
        output_dir=tmp_path / "out",
        output_filename="in.json",
    )

    result = json.loads(output_file.read_text())
    assert result == [
        {
            "element_id": "a",
            "text": "keep me ☃",
            "metadata": {"page_number": 1},
            "conformed_by": "rec-42",
        },
        {
            "element_id": "c",
            "text": "keep me too",
            "metadata": {"page_number": 2},
            "conformed_by": "rec-42",
        },
    ]


def test_process_whole_does_not_load_whole_file(tmp_path: Path, monkeypatch) -> None:
    """Guard against regressing to a whole-file load: the streaming path must not
    call ``json.load`` / ``get_json_data``."""
    input_file = tmp_path / "in.json"
    with input_file.open("w") as f:
        json.dump(ELEMENTS, f, indent=2, ensure_ascii=False)

    def _boom(*args, **kwargs):  # pragma: no cover - only hit on regression
        raise AssertionError("process_whole must not load the whole file via get_json_data")

    # get_json_data is imported into the upload_stager module namespace only if used;
    # patch it at the source module to catch any reintroduction.
    monkeypatch.setattr(data_prep, "get_json_data", _boom)

    output_file = _stager().run(
        elements_filepath=input_file,
        file_data=_file_data(),
        output_dir=tmp_path / "out",
        output_filename="in.json",
    )
    assert len(json.loads(output_file.read_text())) == 2


def test_process_whole_output_matches_ndjson_stream_path(tmp_path: Path) -> None:
    """A ``.json`` input and the equivalent ``.ndjson`` input should yield the
    same conformed elements (the two code paths must stay in lockstep)."""
    json_in = tmp_path / "in.json"
    with json_in.open("w") as f:
        json.dump(ELEMENTS, f)
    ndjson_in = tmp_path / "in.ndjson"
    with ndjson_in.open("w") as f:
        f.write("\n".join(json.dumps(e) for e in ELEMENTS))

    json_out = _stager().run(
        elements_filepath=json_in,
        file_data=_file_data(),
        output_dir=tmp_path / "json_out",
        output_filename="in.json",
    )
    ndjson_out = _stager().run(
        elements_filepath=ndjson_in,
        file_data=_file_data(),
        output_dir=tmp_path / "ndjson_out",
        output_filename="in.ndjson",
    )

    json_elements = json.loads(json_out.read_text())
    ndjson_elements = [json.loads(line) for line in ndjson_out.read_text().splitlines() if line]
    assert json_elements == ndjson_elements


def _make_large_json_array(path: Path, num_elements: int) -> None:
    element = {
        "element_id": "0" * 36,
        "text": "x" * 2000,
        "metadata": {"page_number": 1, "embeddings": [0.123456789] * 100},
    }
    with path.open("w") as f:
        f.write("[\n")
        for i in range(num_elements):
            if i:
                f.write(",\n")
            f.write(json.dumps(element))
        f.write("\n]")


def _peak_bytes_for_run(src: Path, out_dir: Path) -> int:
    stager = _stager()
    tracemalloc.start()
    stager.run(
        elements_filepath=src,
        file_data=_file_data(),
        output_dir=out_dir,
        output_filename=src.name,
    )
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak


def test_process_whole_peak_memory_is_flat_as_input_grows(tmp_path: Path) -> None:
    small_src = tmp_path / "small.json"
    large_src = tmp_path / "large.json"
    _make_large_json_array(small_src, num_elements=1_000)
    _make_large_json_array(large_src, num_elements=10_000)

    assert large_src.stat().st_size > 8 * small_src.stat().st_size

    small_peak = _peak_bytes_for_run(small_src, tmp_path / "small_out")
    large_peak = _peak_bytes_for_run(large_src, tmp_path / "large_out")

    # A whole-file load would make large_peak ~10x small_peak and exceed the file
    # size; streaming keeps the peak dominated by a single element.
    assert large_peak < 3 * small_peak + 1_000_000
    assert large_peak < large_src.stat().st_size


def test_process_whole_rejects_unsupported_suffix(tmp_path: Path) -> None:
    bad = tmp_path / "in.txt"
    bad.write_text("nope")
    with pytest.raises(ValueError):
        _stager().run(
            elements_filepath=bad,
            file_data=_file_data(),
            output_dir=tmp_path / "out",
            output_filename="in.txt",
        )
