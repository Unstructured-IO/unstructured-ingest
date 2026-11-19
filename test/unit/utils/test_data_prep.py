from pathlib import Path

import pytest

from unstructured_ingest.utils.data_prep import (
    generator_batching_wbytes,
    get_json_data,
    write_data,
)


@pytest.fixture
def cp1252_default_encoding(monkeypatch):
    """Simulate Windows, where open() without an encoding decodes as cp1252."""
    original_open = Path.open

    def open_with_cp1252_default(self, mode="r", buffering=-1, encoding=None, *args, **kwargs):
        if "b" not in mode and encoding is None:
            encoding = "cp1252"
        return original_open(self, mode, buffering, encoding, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_with_cp1252_default)


@pytest.mark.parametrize("suffix", [".json", ".ndjson"])
def test_write_and_get_json_data_round_trip_non_ascii(
    tmp_path: Path, cp1252_default_encoding, suffix: str
):
    path = tmp_path / f"data{suffix}"
    data = [{"text": "Привет, мир — 你好 “quoted”"}]

    write_data(path, data)

    assert path.read_bytes().decode("utf-8")
    assert get_json_data(path) == data


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
