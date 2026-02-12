"""Unit tests for SFTP connector.

Tests that SftpUploader creates parent directories before uploading.
SFTP uses a real filesystem where parent directories must exist before a file
can be written. fsspec's SFTPFileSystem.put() bypasses the base class's mkdirs
call (going straight to paramiko), so SftpUploader.run() must call makedirs
explicitly.
"""

from pathlib import Path
from unittest import mock

import pytest

from unstructured_ingest.processes.connectors.fsspec.sftp import SftpUploader


@pytest.fixture
def mock_sftp_client():
    """Create a mock SFTPFileSystem client."""
    client = mock.MagicMock()
    client.makedirs = mock.MagicMock()
    client.upload = mock.MagicMock()
    return client


@pytest.fixture
def sftp_uploader(mock_sftp_client):
    """Create an SftpUploader with mocked dependencies, bypassing __init__."""
    uploader = object.__new__(SftpUploader)
    uploader.connector_type = "sftp"

    # Mock connection config — get_client yields the mock SFTP client
    uploader.connection_config = mock.MagicMock()
    uploader.connection_config.get_client.return_value.__enter__ = mock.MagicMock(
        return_value=mock_sftp_client
    )
    uploader.connection_config.get_client.return_value.__exit__ = mock.MagicMock(
        return_value=False
    )

    # Mock upload config
    uploader.upload_config = mock.MagicMock()
    uploader.upload_config.protocol = "sftp"
    uploader.upload_config.path_without_protocol = "data/output"

    # Silence logging methods inherited from ConnectorLoggingMixin
    uploader.log_upload_start = mock.MagicMock()
    uploader.log_upload_complete = mock.MagicMock()
    uploader.log_error = mock.MagicMock()

    return uploader


def _make_file_data(relative_path: str):
    """Create a mock FileData with the given relative_path."""
    file_data = mock.MagicMock()
    file_data.source_identifiers.relative_path = relative_path
    return file_data


class TestSftpUploaderMakeDirs:
    """Verify that SftpUploader.run() creates parent directories before uploading."""

    def test_makedirs_called_with_parent_of_upload_path(
        self, sftp_uploader, mock_sftp_client, tmp_path
    ):
        """makedirs should receive the parent directory of the upload destination."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        file_data = _make_file_data("subdir/nested/report.pdf")
        sftp_uploader.run(path=local_file, file_data=file_data)

        mock_sftp_client.makedirs.assert_called_once()
        parent_path = mock_sftp_client.makedirs.call_args[0][0]
        assert parent_path == "data/output/subdir/nested"
        assert mock_sftp_client.makedirs.call_args[1] == {"exist_ok": True}

    def test_makedirs_called_before_upload(self, sftp_uploader, mock_sftp_client, tmp_path):
        """makedirs must execute before upload — ordering matters on a real FS."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        call_order = []
        mock_sftp_client.makedirs.side_effect = lambda *a, **kw: call_order.append("makedirs")
        mock_sftp_client.upload.side_effect = lambda **kw: call_order.append("upload")

        sftp_uploader.run(path=local_file, file_data=_make_file_data("deep/path/file.txt"))

        assert call_order == ["makedirs", "upload"]

    def test_flat_file_still_calls_makedirs(self, sftp_uploader, mock_sftp_client, tmp_path):
        """Even a file with no subdirectories should call makedirs (on the base dir)."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        sftp_uploader.run(path=local_file, file_data=_make_file_data("flat_file.pdf"))

        mock_sftp_client.makedirs.assert_called_once()
        parent_path = mock_sftp_client.makedirs.call_args[0][0]
        assert parent_path == "data/output"

    def test_upload_receives_correct_paths(self, sftp_uploader, mock_sftp_client, tmp_path):
        """upload should still receive the correct local and remote paths."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        sftp_uploader.run(path=local_file, file_data=_make_file_data("subdir/file.txt"))

        mock_sftp_client.upload.assert_called_once()
        call_kwargs = mock_sftp_client.upload.call_args[1]
        assert call_kwargs["lpath"] == str(local_file.resolve())
        assert "subdir/file.txt.json" in call_kwargs["rpath"]

    def test_error_propagated_after_makedirs(self, sftp_uploader, mock_sftp_client, tmp_path):
        """If upload fails, the error should still propagate via wrap_error."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        mock_sftp_client.upload.side_effect = IOError("SFTP write failed")

        with pytest.raises(Exception):
            sftp_uploader.run(path=local_file, file_data=_make_file_data("dir/file.txt"))

        # makedirs was still called before the failure
        mock_sftp_client.makedirs.assert_called_once()


class TestSftpUploaderMakeDirsAsync:
    """Verify the async path delegates to run() (which has the makedirs fix).

    SFTPFileSystem.async_impl is False, so the pipeline never calls run_async()
    for SFTP. But we override it to delegate to run() so the fix is present
    even if someone calls run_async() directly.
    """

    @pytest.mark.asyncio
    async def test_run_async_delegates_to_run(self, sftp_uploader, mock_sftp_client, tmp_path):
        """run_async should delegate to run(), inheriting the makedirs call."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        await sftp_uploader.run_async(
            path=local_file, file_data=_make_file_data("nested/dir/file.txt")
        )

        # makedirs is called because run_async delegates to run()
        mock_sftp_client.makedirs.assert_called_once()
        parent_path = mock_sftp_client.makedirs.call_args[0][0]
        assert parent_path == "data/output/nested/dir"
        assert mock_sftp_client.makedirs.call_args[1] == {"exist_ok": True}
        mock_sftp_client.upload.assert_called_once()
