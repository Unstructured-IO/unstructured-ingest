"""Tests for the bounded-memory streaming helpers in ``data_prep``.

These cover two guarantees:

1. **Output equivalence** - ``json_stream`` + ``write_data_streaming`` produce
   byte-for-byte identical output to the old ``json.load`` + ``write_data`` path,
   so the streaming rewrite of ``UploadStager.process_whole`` is a pure
   drop-in.
2. **Bounded memory** - the streaming path's peak allocation stays roughly flat
   as the input grows, i.e. it does NOT load the whole file (the OOM that killed
   the 48k-page stager).
"""

import json
import tracemalloc
from pathlib import Path

import pytest

from unstructured_ingest.utils import ndjson
from unstructured_ingest.utils.data_prep import (
    json_stream,
    write_data,
    write_data_streaming,
)

SAMPLE_ELEMENTS = [
    {
        "element_id": "abc123",
        "text": 'Héllo wörld with non-ascii ☃ and "quotes"',
        "type": "NarrativeText",
        "metadata": {
            "page_number": 1,
            "coordinates": {"points": [[0.0, 1.5], [2.25, 3.0]], "system": "Pixel"},
            "scores": [0.1, 0.2, 0.3],
            "languages": ["eng"],
            "nested": {"a": None, "b": True, "c": 42, "d": 3.14159},
        },
    },
    {"element_id": "def456", "text": "", "metadata": {}},
    {"element_id": "ghi789", "text": "tab\tand\nnewline", "metadata": {"page_number": 12}},
]


def _write_json(path: Path, data: list[dict], indent) -> None:
    with path.open("w") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


@pytest.mark.parametrize("indent", [2, 4, None])
def test_write_data_streaming_json_matches_write_data(tmp_path: Path, indent) -> None:
    streamed = tmp_path / "streamed.json"
    whole = tmp_path / "whole.json"

    write_data_streaming(path=streamed, data=iter(SAMPLE_ELEMENTS), indent=indent)
    write_data(path=whole, data=SAMPLE_ELEMENTS, indent=indent)

    assert streamed.read_text() == whole.read_text()
    # And it is still valid JSON that round-trips to the original elements.
    assert json.loads(streamed.read_text()) == SAMPLE_ELEMENTS


def test_write_data_streaming_empty_json_matches_write_data(tmp_path: Path) -> None:
    streamed = tmp_path / "streamed.json"
    whole = tmp_path / "whole.json"

    write_data_streaming(path=streamed, data=iter([]), indent=2)
    write_data(path=whole, data=[], indent=2)

    assert streamed.read_text() == whole.read_text() == "[]"


def test_write_data_streaming_ndjson_matches_write_data(tmp_path: Path) -> None:
    streamed = tmp_path / "streamed.ndjson"
    whole = tmp_path / "whole.ndjson"

    write_data_streaming(path=streamed, data=iter(SAMPLE_ELEMENTS))
    write_data(path=whole, data=SAMPLE_ELEMENTS)

    assert streamed.read_text() == whole.read_text()
    assert ndjson.loads(streamed.read_text()) == SAMPLE_ELEMENTS


def test_write_data_streaming_rejects_unknown_suffix(tmp_path: Path) -> None:
    with pytest.raises(IOError):
        write_data_streaming(path=tmp_path / "out.csv", data=iter(SAMPLE_ELEMENTS))


def test_json_stream_yields_same_elements_as_json_load(tmp_path: Path) -> None:
    path = tmp_path / "elements.json"
    _write_json(path, SAMPLE_ELEMENTS, indent=2)

    assert list(json_stream(path=path)) == SAMPLE_ELEMENTS


def test_json_stream_is_lazy_generator(tmp_path: Path) -> None:
    path = tmp_path / "elements.json"
    _write_json(path, SAMPLE_ELEMENTS, indent=2)

    stream = json_stream(path=path)
    # First pull yields the first element without consuming the rest.
    assert next(stream) == SAMPLE_ELEMENTS[0]


def _make_large_json_array(path: Path, num_elements: int) -> None:
    """Write a JSON array of fat element dicts directly to disk (never holding
    the whole list in memory, so the test itself does not blow up)."""
    big_text = "x" * 2000
    element = {
        "element_id": "0" * 36,
        "text": big_text,
        "type": "NarrativeText",
        "metadata": {"page_number": 1, "embeddings": [0.123456789] * 100},
    }
    with path.open("w") as f:
        f.write("[\n")
        for i in range(num_elements):
            if i:
                f.write(",\n")
            f.write(json.dumps(element))
        f.write("\n]")


def _peak_bytes_streaming_copy(src: Path, dst: Path) -> int:
    tracemalloc.start()
    write_data_streaming(
        path=dst,
        data=(element for element in json_stream(path=src)),
    )
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak


def test_streaming_copy_peak_memory_is_flat_as_input_grows(tmp_path: Path) -> None:
    """The whole point: peak memory must NOT scale with the number of elements.

    A ~per-element-bounded implementation has a peak dominated by a single
    element (a few KB); a json.load-based implementation has a peak that scales
    with the entire file (tens of MB here). Assert the large input's peak does
    not grow proportionally to its 10x element count.
    """
    small_src = tmp_path / "small.json"
    large_src = tmp_path / "large.json"
    _make_large_json_array(small_src, num_elements=1_000)
    _make_large_json_array(large_src, num_elements=10_000)

    # Sanity: the large file really is ~10x bigger on disk.
    assert large_src.stat().st_size > 8 * small_src.stat().st_size

    small_peak = _peak_bytes_streaming_copy(small_src, tmp_path / "small_out.json")
    large_peak = _peak_bytes_streaming_copy(large_src, tmp_path / "large_out.json")

    # If the implementation buffered the whole file, large_peak would be ~10x
    # small_peak (and tens of MB). Streaming keeps both peaks dominated by a
    # single element, so the large peak must stay well under 3x the small peak
    # and comfortably below the on-disk size of the large input.
    assert large_peak < 3 * small_peak + 1_000_000
    assert large_peak < large_src.stat().st_size
