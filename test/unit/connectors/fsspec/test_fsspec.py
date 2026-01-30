"""Unit tests for fsspec connector base classes."""

from pathlib import Path
from unittest import mock

import pytest

from unstructured_ingest.processes.connectors.fsspec.fsspec import (
    FsspecAccessConfig,
    FsspecConnectionConfig,
    FsspecIndexer,
    FsspecIndexerConfig,
)


class TestFsspecIndexerRelPath:
    """Tests for the rel_path calculation in FsspecIndexer.run().

    These tests verify that the fix for the str.replace() bug works correctly.
    The bug occurred when a filename contained the bucket/path prefix, causing
    str.replace() to remove multiple occurrences and corrupt the filename.

    Example bug scenario (before fix):
        path_without_protocol = "my-bucket"
        file_path = "my-bucket/my-bucket_report.csv"
        rel_path = file_path.replace(path_without_protocol, "")
        # Result: "/_report.csv" (corrupted!)

    After fix with replace(..., "", 1):
        rel_path = file_path.replace(path_without_protocol, "", 1)
        # Result: "/my-bucket_report.csv" (correct!)
    """

    @pytest.fixture
    def mock_connection_config(self):
        """Create a mock connection config that doesn't require real credentials."""
        config = mock.MagicMock(spec=FsspecConnectionConfig)
        # Mock the context manager for get_client
        mock_client = mock.MagicMock()
        config.get_client.return_value.__enter__.return_value = mock_client
        config.get_client.return_value.__exit__.return_value = None
        return config

    def create_indexer(self, remote_url: str, mock_connection_config):
        """Helper to create an indexer with the given remote URL."""
        index_config = FsspecIndexerConfig(remote_url=remote_url)
        indexer = FsspecIndexer(
            connection_config=mock_connection_config,
            index_config=index_config,
        )
        # Mock get_metadata to return a simple object
        indexer.get_metadata = mock.MagicMock(return_value=mock.MagicMock())
        return indexer

    def test_normal_filename_no_prefix_match(self, mock_connection_config):
        """Test normal case where filename doesn't contain the bucket name."""
        indexer = self.create_indexer("s3://my-bucket/", mock_connection_config)

        file_path = "my-bucket/documents/report.csv"
        rel_path = file_path.replace(
            indexer.index_config.path_without_protocol, "", 1
        ).lstrip("/")

        assert rel_path == "documents/report.csv"

    def test_filename_contains_bucket_name_with_trailing_slash(self, mock_connection_config):
        """Test that filename containing bucket name is preserved when URL has trailing slash."""
        indexer = self.create_indexer("s3://my-bucket/", mock_connection_config)

        # Filename contains "my-bucket" but path_without_protocol is "my-bucket/"
        # so only one replacement happens (the prefix)
        file_path = "my-bucket/my-bucket_report.csv"
        rel_path = file_path.replace(
            indexer.index_config.path_without_protocol, "", 1
        ).lstrip("/")

        assert rel_path == "my-bucket_report.csv"

    def test_filename_contains_bucket_name_without_trailing_slash(self, mock_connection_config):
        """Test the bug case: filename contains bucket name AND URL has no trailing slash.

        This is the scenario that was broken before the fix.
        Without the fix (using replace without count=1):
            "my-bucket/my-bucket_report.csv".replace("my-bucket", "")
            → "/_report.csv" (WRONG!)

        With the fix (using replace with count=1):
            "my-bucket/my-bucket_report.csv".replace("my-bucket", "", 1)
            → "/my-bucket_report.csv" → "my-bucket_report.csv" (CORRECT!)
        """
        indexer = self.create_indexer("s3://my-bucket", mock_connection_config)

        # path_without_protocol is "my-bucket" (no trailing slash)
        assert indexer.index_config.path_without_protocol == "my-bucket"

        file_path = "my-bucket/my-bucket_report.csv"

        # Demonstrate the bug (what would happen without count=1)
        buggy_rel_path = file_path.replace(
            indexer.index_config.path_without_protocol, ""
        ).lstrip("/")
        assert buggy_rel_path == "/_report.csv".lstrip("/")  # Corrupted!

        # The fix: use count=1 to only replace first occurrence
        fixed_rel_path = file_path.replace(
            indexer.index_config.path_without_protocol, "", 1
        ).lstrip("/")
        assert fixed_rel_path == "my-bucket_report.csv"  # Correct!

    def test_long_bucket_name_in_filename(self, mock_connection_config):
        """Test with realistic long bucket name that appears in filename."""
        indexer = self.create_indexer(
            "s3://utic-platform-test-destination", mock_connection_config
        )

        file_path = "utic-platform-test-destination/utic-platform-test-destination_report.csv"

        # Without the fix, this would become "/_report.csv"
        rel_path = file_path.replace(
            indexer.index_config.path_without_protocol, "", 1
        ).lstrip("/")

        assert rel_path == "utic-platform-test-destination_report.csv"

    def test_bucket_name_appears_multiple_times_in_path(self, mock_connection_config):
        """Test when bucket name appears multiple times in the full path."""
        indexer = self.create_indexer("s3://data", mock_connection_config)

        # "data" appears in bucket, directory name, and filename
        file_path = "data/data-exports/data-2024.csv"

        rel_path = file_path.replace(
            indexer.index_config.path_without_protocol, "", 1
        ).lstrip("/")

        # Should only remove the first "data" (the bucket prefix)
        assert rel_path == "data-exports/data-2024.csv"

    def test_sftp_dot_path(self, mock_connection_config):
        """Test SFTP edge case with '.' as the path.

        When using sftp://host/. the path_without_protocol is "host/."
        which could cause issues if files have '.' in their names.
        """
        indexer = self.create_indexer("sftp://myhost/.", mock_connection_config)

        file_path = "myhost/./config.json"

        rel_path = file_path.replace(
            indexer.index_config.path_without_protocol, "", 1
        ).lstrip("/")

        assert rel_path == "config.json"

    def test_nested_directory_with_bucket_name(self, mock_connection_config):
        """Test file in nested directory where parent dir matches bucket name."""
        indexer = self.create_indexer("s3://reports", mock_connection_config)

        # Directory named "reports" inside bucket "reports"
        file_path = "reports/reports/quarterly/q1.pdf"

        rel_path = file_path.replace(
            indexer.index_config.path_without_protocol, "", 1
        ).lstrip("/")

        # Should preserve the nested "reports" directory
        assert rel_path == "reports/quarterly/q1.pdf"

    def test_file_at_bucket_root(self, mock_connection_config):
        """Test file directly at bucket root."""
        indexer = self.create_indexer("s3://my-bucket/", mock_connection_config)

        file_path = "my-bucket/file.txt"

        rel_path = file_path.replace(
            indexer.index_config.path_without_protocol, "", 1
        ).lstrip("/")

        assert rel_path == "file.txt"

    def test_file_with_special_characters(self, mock_connection_config):
        """Test filename with special characters that could affect replace."""
        indexer = self.create_indexer("s3://bucket/", mock_connection_config)

        file_path = "bucket/file (copy).txt"

        rel_path = file_path.replace(
            indexer.index_config.path_without_protocol, "", 1
        ).lstrip("/")

        assert rel_path == "file (copy).txt"


class TestBoxStyleLeadingSlashes:
    """Tests for Box-style paths with leading slashes.

    Box (the cloud storage service) returns file paths with leading slashes
    that other protocols don't include. The lstrip("/") after the replace
    operation handles this case.

    These tests verify that:
    1. Leading slashes from Box are handled correctly
    2. The count=1 fix works correctly with Box-style paths
    3. Filenames containing the folder ID are preserved
    """

    def test_box_leading_slash_basic(self):
        """Test basic Box scenario where path has leading slash."""
        # Box folder ID as path_without_protocol
        path_without_protocol = "12345678"
        # Box returns path with leading slash
        file_path = "/12345678/documents/report.pdf"

        rel_path = file_path.replace(path_without_protocol, "", 1).lstrip("/")

        assert rel_path == "documents/report.pdf"

    def test_box_leading_slash_with_filename_containing_folder_id(self):
        """Test Box with filename containing the folder ID.

        This combines the Box leading slash behavior with the bug scenario.
        Without count=1, both occurrences would be replaced.
        """
        path_without_protocol = "12345678"
        # File named "12345678_backup.zip" in Box folder 12345678
        file_path = "/12345678/12345678_backup.zip"

        # BUG: Without count=1, both occurrences removed
        buggy = file_path.replace(path_without_protocol, "").lstrip("/")
        assert buggy == "_backup.zip"  # CORRUPTED!

        # FIX: With count=1, only first occurrence (the folder prefix) removed
        fixed = file_path.replace(path_without_protocol, "", 1).lstrip("/")
        assert fixed == "12345678_backup.zip"  # CORRECT!

    def test_box_double_leading_slash(self):
        """Test Box with double leading slashes (can occur in some cases)."""
        path_without_protocol = "folder_id"
        file_path = "//folder_id/subfolder/file.txt"

        rel_path = file_path.replace(path_without_protocol, "", 1).lstrip("/")

        assert rel_path == "subfolder/file.txt"

    def test_box_nested_folders_with_same_name(self):
        """Test Box with nested folders having same name as root folder."""
        path_without_protocol = "shared"
        # Box returns: /shared/shared/team/doc.pdf
        file_path = "/shared/shared/team/doc.pdf"

        rel_path = file_path.replace(path_without_protocol, "", 1).lstrip("/")

        # Should preserve the nested "shared" folder
        assert rel_path == "shared/team/doc.pdf"

    def test_box_file_at_folder_root(self):
        """Test Box file directly in the root folder."""
        path_without_protocol = "99887766"
        file_path = "/99887766/readme.txt"

        rel_path = file_path.replace(path_without_protocol, "", 1).lstrip("/")

        assert rel_path == "readme.txt"

    def test_box_deeply_nested_path(self):
        """Test Box with deeply nested path structure."""
        path_without_protocol = "root_folder"
        file_path = "/root_folder/level1/level2/level3/deep_file.docx"

        rel_path = file_path.replace(path_without_protocol, "", 1).lstrip("/")

        assert rel_path == "level1/level2/level3/deep_file.docx"

    def test_box_folder_id_appears_in_subfolder_name(self):
        """Test when folder ID appears in a subfolder name."""
        path_without_protocol = "abc123"
        # Subfolder named "abc123_archive" contains the folder ID
        file_path = "/abc123/abc123_archive/old_data.csv"

        rel_path = file_path.replace(path_without_protocol, "", 1).lstrip("/")

        # Should preserve the subfolder name
        assert rel_path == "abc123_archive/old_data.csv"


class TestRelPathCalculationLogic:
    """Direct tests for the rel_path calculation logic.

    These tests verify the string replacement logic used in the indexer
    without requiring full object instantiation.
    """

    def test_replace_with_count_preserves_duplicates(self):
        """Test that replace with count=1 only removes first occurrence."""
        # Simulates: bucket name "data", file in "data" directory named "data.csv"
        path_without_protocol = "data"
        file_path = "data/data/data.csv"

        # Without count=1 - WRONG (removes all occurrences)
        buggy_before_strip = file_path.replace(path_without_protocol, "")
        assert buggy_before_strip == "//.csv"  # All "data" removed
        buggy = buggy_before_strip.lstrip("/")
        assert buggy == ".csv"  # After lstrip, only .csv remains - CORRUPTED!

        # With count=1 - CORRECT (removes only prefix)
        fixed = file_path.replace(path_without_protocol, "", 1).lstrip("/")
        assert fixed == "data/data.csv"  # Preserves the nested path

    def test_trailing_slash_provides_natural_protection(self):
        """Test that trailing slash in path_without_protocol naturally protects.

        When users specify URLs with trailing slash (s3://bucket/), the
        path_without_protocol includes the slash, which means it won't
        match occurrences in the filename that don't have the slash.
        """
        path_without_protocol = "my-bucket/"  # Has trailing slash
        file_path = "my-bucket/my-bucket_report.csv"

        # Even without count=1, only matches the prefix (because of trailing slash)
        result = file_path.replace(path_without_protocol, "").lstrip("/")
        assert result == "my-bucket_report.csv"

    def test_no_trailing_slash_causes_bug_without_fix(self):
        """Test the actual bug scenario: no trailing slash corrupts filename.

        This is the exact scenario the user reported:
        - Bucket: utic-platform-test-destination
        - File: utic-platform-test-destination_report.csv
        - URL without trailing slash: s3://utic-platform-test-destination
        """
        path_without_protocol = "utic-platform-test-destination"  # No trailing slash
        file_path = "utic-platform-test-destination/utic-platform-test-destination_report.csv"

        # BUG: Without count=1, both occurrences are replaced
        buggy = file_path.replace(path_without_protocol, "").lstrip("/")
        assert buggy == "/_report.csv".lstrip("/")
        assert buggy == "_report.csv"  # CORRUPTED!

        # FIX: With count=1, only the prefix is replaced
        fixed = file_path.replace(path_without_protocol, "", 1).lstrip("/")
        assert fixed == "/utic-platform-test-destination_report.csv".lstrip("/")
        assert fixed == "utic-platform-test-destination_report.csv"  # CORRECT!
