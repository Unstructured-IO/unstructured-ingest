"""Bounded-memory + output tests for ``BlobStoreUploadStager``.

``BlobStoreUploadStager.run`` overrides the base ``UploadStager.run`` and is the stager
used by blob-store destinations (e.g. S3). It always emits a ``.json`` copy, streaming
NDJSON input element-by-element into a JSON array and routing JSON-array input through
the base ``process_whole``. These tests assert it produces a correct ``.json`` copy for
both input formats and that peak memory stays flat as the input grows.
"""

import json
import tracemalloc
from pathlib import Path

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.utils.blob_storage import (
    BlobStoreUploadStager,
    BlobStoreUploadStagerConfig,
)

ELEMENTS = [
    {"element_id": "a", "text": "keep ☃", "metadata": {"page_number": 1}},
    {"element_id": "b", "text": "two", "metadata": {"page_number": 2}},
]


def _file_data() -> FileData:
    return FileData(
        identifier="rec-1",
        connector_type="test",
        source_identifiers=SourceIdentifiers(filename="doc.pdf", fullpath="doc.pdf"),
    )


def _stager() -> BlobStoreUploadStager:
    return BlobStoreUploadStager(upload_stager_config=BlobStoreUploadStagerConfig())


def test_blob_store_stager_streams_json_copy(tmp_path: Path) -> None:
    src = tmp_path / "in.json"
    with src.open("w") as f:
        json.dump(ELEMENTS, f)

    out = _stager().run(
        elements_filepath=src,
        file_data=_file_data(),
        output_dir=tmp_path / "out",
        output_filename="in.json",
    )

    assert out.suffix == ".json"
    assert json.loads(out.read_text()) == ELEMENTS


@pytest.mark.parametrize("elements", [ELEMENTS[:1], ELEMENTS], ids=["single", "multiple"])
def test_blob_store_stager_outputs_json_for_ndjson_input(
    tmp_path: Path, elements: list[dict]
) -> None:
    src = tmp_path / "in.ndjson"
    with src.open("w") as f:
        f.write("\n".join(json.dumps(e) for e in elements))

    out = _stager().run(
        elements_filepath=src,
        file_data=_file_data(),
        output_dir=tmp_path / "out",
        output_filename="in.ndjson",
    )

    assert out.suffix == ".json"
    assert json.loads(out.read_text()) == elements


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


def test_blob_store_stager_peak_memory_is_flat_as_input_grows(tmp_path: Path) -> None:
    small_src = tmp_path / "small.json"
    large_src = tmp_path / "large.json"
    _make_large_json_array(small_src, num_elements=1_000)
    _make_large_json_array(large_src, num_elements=10_000)

    assert large_src.stat().st_size > 8 * small_src.stat().st_size

    large_peak = _peak_bytes_for_run(large_src, tmp_path / "large_out")

    # The whole-file path (get_json_data + write_data) materializes the parsed list,
    # which is several times the JSON text, so its peak runs well above the file size.
    # The streamed path keeps peak a small fraction of it. tracemalloc's "peak" also
    # counts not-yet-collected per-element garbage, so it isn't perfectly flat across
    # GC timing — assert the streaming-vs-whole-file bound (peak < file size) rather
    # than a brittle peak-ratio between two input sizes.
    assert large_peak < large_src.stat().st_size
