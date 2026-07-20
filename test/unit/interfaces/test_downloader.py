from datetime import datetime, timezone
from pathlib import Path

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.interfaces.downloader import Downloader, DownloaderConfig


class DummyDownloader(Downloader):
    connector_type: str = "test"
    download_config: DownloaderConfig = DownloaderConfig()

    def run(self, file_data: FileData, **kwargs):
        raise NotImplementedError()


def make_file_data(**metadata: str) -> FileData:
    return FileData(
        identifier="id",
        connector_type="test",
        source_identifiers=SourceIdentifiers(filename="f.txt", fullpath="f.txt"),
        metadata=metadata,
    )


@pytest.fixture()
def downloaded_file(tmp_path: Path) -> Path:
    path = tmp_path / "f.txt"
    path.write_text("downloaded content")
    return path


REMOTE_MODIFIED = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc).timestamp()


@pytest.mark.parametrize(
    "date_modified",
    ["1700000000", "2023-11-14T22:13:20Z", "2023-11-14T17:13:20-05:00"],
)
def test_downloaded_file_mtime_matches_remote_modified_time(
    downloaded_file: Path, date_modified: str
):
    downloader = DummyDownloader()
    file_data = make_file_data(date_modified=date_modified)

    downloader.generate_download_response(file_data=file_data, download_path=downloaded_file)

    assert downloaded_file.stat().st_mtime == REMOTE_MODIFIED


def test_downloaded_file_mtime_is_set_without_a_parseable_date_created(downloaded_file: Path):
    downloader = DummyDownloader()
    file_data = make_file_data(date_modified="2023-11-14T22:13:20Z", date_created="unknown")

    downloader.generate_download_response(file_data=file_data, download_path=downloaded_file)

    assert downloaded_file.stat().st_mtime == REMOTE_MODIFIED


def test_downloaded_file_atime_matches_remote_created_time(downloaded_file: Path):
    downloader = DummyDownloader()
    file_data = make_file_data(
        date_modified="2023-11-14T22:13:20Z", date_created="2023-11-13T22:13:20Z"
    )

    downloader.generate_download_response(file_data=file_data, download_path=downloaded_file)

    assert downloaded_file.stat().st_atime == REMOTE_MODIFIED - 86400


def test_downloaded_file_keeps_its_own_mtime_when_remote_time_is_unparseable(
    downloaded_file: Path,
):
    downloader = DummyDownloader()
    original_mtime = downloaded_file.stat().st_mtime
    file_data = make_file_data(date_modified="Monday")

    downloader.generate_download_response(file_data=file_data, download_path=downloaded_file)

    assert downloaded_file.stat().st_mtime == original_mtime
