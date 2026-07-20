import io
import logging
import tarfile
from pathlib import Path

import pytest

from unstructured_ingest.utils.compression import uncompress_tar_file


def _write_tar_member(archive_path: Path, member_name: str, content: bytes) -> None:
    with tarfile.open(archive_path, "w") as archive:
        member = tarfile.TarInfo(member_name)
        member.size = len(content)
        archive.addfile(member, io.BytesIO(content))


def test_uncompress_tar_file_extracts_benign_members(tmp_path: Path) -> None:
    archive_path = tmp_path / "benign.tar"
    destination = tmp_path / "output"
    _write_tar_member(archive_path, "nested/content.txt", b"content")

    uncompress_tar_file(str(archive_path), str(destination))

    assert (destination / "nested/content.txt").read_bytes() == b"content"


@pytest.mark.skipif(
    not hasattr(tarfile, "tar_filter"),
    reason="tar extraction filters are unavailable in this Python patch release",
)
def test_uncompress_tar_file_rejects_path_traversal(tmp_path: Path) -> None:
    archive_path = tmp_path / "unsafe.tar"
    destination = tmp_path / "output"
    _write_tar_member(archive_path, "../escaped.txt", b"must not escape")

    with pytest.raises(tarfile.FilterError):
        uncompress_tar_file(str(archive_path), str(destination))

    assert not (tmp_path / "escaped.txt").exists()


def test_uncompress_tar_file_warns_when_filter_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    archive_path = tmp_path / "benign.tar"
    destination = tmp_path / "output"
    _write_tar_member(archive_path, "content.txt", b"content")
    monkeypatch.delattr(tarfile, "tar_filter", raising=False)

    with caplog.at_level(logging.WARNING, logger="unstructured_ingest"):
        uncompress_tar_file(str(archive_path), str(destination))

    assert (destination / "content.txt").read_bytes() == b"content"
    assert "Extraction filtering for tar files is unavailable" in caplog.text
