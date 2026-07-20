from pathlib import Path

from unstructured_ingest.utils.data_prep import get_json_data


def test_get_json_data_reads_ndjson_from_unrecognized_extension(tmp_path: Path):
    path = tmp_path / "data.tmp"
    path.write_text('{"a": 1}\n{"a": 2}\n')

    assert get_json_data(path) == [{"a": 1}, {"a": 2}]
