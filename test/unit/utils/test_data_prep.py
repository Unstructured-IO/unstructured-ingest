from pathlib import Path

from unstructured_ingest.utils.data_prep import generator_batching_wbytes, get_json_data


def test_get_json_data_reads_ndjson_from_unrecognized_extension(tmp_path: Path):
    path = tmp_path / "data.tmp"
    path.write_text('{"a": 1}\n{"a": 2}\n')

    assert get_json_data(path) == [{"a": 1}, {"a": 2}]


def test_generator_batching_wbytes_without_limits_yields_one_batch():
    items = [{"a": 1}, {"a": 2}, {"a": 3}]

    assert list(generator_batching_wbytes(items)) == [items]
    assert list(generator_batching_wbytes(items, batch_size_limit_bytes=0)) == [items]


def test_generator_batching_wbytes_does_not_yield_an_empty_batch_for_an_oversized_item():
    big, small = {"a": "x" * 100}, {"a": "y"}

    assert list(generator_batching_wbytes([big, small], batch_size_limit_bytes=20)) == [
        [big],
        [small],
    ]
    assert list(generator_batching_wbytes([small, big], batch_size_limit_bytes=20)) == [
        [small],
        [big],
    ]
