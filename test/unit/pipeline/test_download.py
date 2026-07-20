import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.pipeline.steps.download import DownloadStep
from unstructured_ingest.utils.string_and_date_utils import parse_timestamp


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1700000000", 1700000000.0),
        ("1700000000.5", 1700000000.5),
        ("2023-11-14T22:13:20Z", 1700000000.0),
        ("2023-11-14T17:13:20-05:00", 1700000000.0),
        ("2023-11-14T22:13:20", 1700000000.0),
        (None, None),
        ("Monday", None),
        ("not a date", None),
        ("", None),
    ],
)
def test_parse_timestamp_handles_epoch_and_iso_values(value, expected):
    assert parse_timestamp(value) == expected


def make_step(download_path: Path) -> DownloadStep:
    process = Mock()
    process.download_config = None
    process.connection_config = None
    process.get_download_path.return_value = download_path
    return DownloadStep(process=process, context=ProcessorConfig(re_download=False))


def make_file_data(date_modified: str) -> FileData:
    return FileData(
        identifier="id",
        connector_type="test",
        source_identifiers=SourceIdentifiers(filename="f.txt", fullpath="f.txt"),
        metadata={"date_modified": date_modified},
    )


@pytest.fixture()
def local_file(tmp_path: Path) -> Path:
    path = tmp_path / "f.txt"
    path.write_text("local content")
    mtime = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc).timestamp()
    os.utime(path, times=(mtime, mtime))
    return path


def test_should_download_when_remote_is_newer_than_local_copy(local_file, tmp_path):
    step = make_step(local_file)
    newer = datetime(2023, 11, 15, 22, 13, 20, tzinfo=timezone.utc).timestamp()
    file_data = make_file_data(str(newer))
    file_data_path = tmp_path / "file_data.json"
    file_data.to_file(path=str(file_data_path))

    assert step.should_download(file_data=file_data, file_data_path=str(file_data_path)) is True
    assert file_data.reprocess is True


def test_should_not_download_when_local_copy_is_up_to_date(local_file, tmp_path):
    step = make_step(local_file)
    older = datetime(2023, 11, 13, 22, 13, 20, tzinfo=timezone.utc).timestamp()
    file_data = make_file_data(str(older))
    file_data_path = tmp_path / "file_data.json"

    assert step.should_download(file_data=file_data, file_data_path=str(file_data_path)) is False


def test_should_download_when_iso_timestamped_remote_is_newer(local_file, tmp_path):
    step = make_step(local_file)
    file_data = make_file_data("2023-11-15T22:13:20Z")
    file_data_path = tmp_path / "file_data.json"
    file_data.to_file(path=str(file_data_path))

    assert step.should_download(file_data=file_data, file_data_path=str(file_data_path)) is True


def test_should_not_download_when_date_modified_is_unparseable(local_file, tmp_path):
    step = make_step(local_file)
    file_data = make_file_data("Monday")
    file_data_path = tmp_path / "file_data.json"

    assert step.should_download(file_data=file_data, file_data_path=str(file_data_path)) is False
    assert file_data.reprocess is False


def test_naive_iso_timestamp_is_treated_as_utc():
    naive = parse_timestamp("2023-11-14T22:13:20")
    explicit_utc = parse_timestamp("2023-11-14T22:13:20+00:00")
    assert naive == explicit_utc


def test_local_timezone_does_not_shift_iso_timestamps(monkeypatch):
    before = parse_timestamp("2023-11-14T22:13:20")
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    if hasattr(os, "tzset"):
        os.tzset()
    try:
        assert parse_timestamp("2023-11-14T22:13:20") == before
    finally:
        monkeypatch.undo()
        if hasattr(os, "tzset"):
            os.tzset()


def test_reprocess_flag_persists_to_file_data_file(local_file, tmp_path):
    step = make_step(local_file)
    newer = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc) + timedelta(days=1)
    file_data = make_file_data(newer.isoformat())
    file_data_path = tmp_path / "file_data.json"
    file_data.to_file(path=str(file_data_path))

    step.should_download(file_data=file_data, file_data_path=str(file_data_path))

    assert FileData.from_file(path=str(file_data_path)).reprocess is True
