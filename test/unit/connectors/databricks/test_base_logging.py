import json
from dataclasses import dataclass
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.processes.connectors.databricks.volumes import (
    DatabricksVolumesAccessConfig,
    DatabricksVolumesConnectionConfig,
    DatabricksVolumesDownloader,
    DatabricksVolumesDownloaderConfig,
    DatabricksVolumesIndexer,
    DatabricksVolumesIndexerConfig,
    DatabricksVolumesUploader,
    DatabricksVolumesUploaderConfig,
)


# Create concrete test implementations since the base classes are abstract
@dataclass
class TestDatabricksVolumesIndexer(DatabricksVolumesIndexer):
    connector_type: str = "test_databricks_volumes"


@dataclass
class TestDatabricksVolumesDownloader(DatabricksVolumesDownloader):
    connector_type: str = "test_databricks_volumes"


@dataclass
class TestDatabricksVolumesUploader(DatabricksVolumesUploader):
    connector_type: str = "test_databricks_volumes"


class TestDatabricksVolumesLogging:
    """Test class focusing on logging functionality of Databricks volumes connector."""

    @pytest.fixture
    def mock_workspace_client(self):
        """Create a mock WorkspaceClient."""
        mock_client = MagicMock()
        yield mock_client

    @pytest.fixture
    def connection_config(self):
        """Create a test connection config."""
        access_config = DatabricksVolumesAccessConfig(token="test-token")
        return DatabricksVolumesConnectionConfig(
            host="https://test.databricks.com", access_config=access_config
        )

    @pytest.fixture
    def indexer_config(self):
        """Create a test indexer config."""
        return DatabricksVolumesIndexerConfig(
            volume="test_volume",
            catalog="test_catalog",
            volume_path="test_path",
            databricks_schema="test_schema",
            recursive=True,
        )

    @pytest.fixture
    def downloader_config(self, tmp_path):
        """Create a test downloader config."""
        return DatabricksVolumesDownloaderConfig(download_dir=tmp_path)

    @pytest.fixture
    def uploader_config(self):
        """Create a test uploader config."""
        return DatabricksVolumesUploaderConfig(
            volume="test_volume",
            catalog="test_catalog",
            volume_path="test_path",
            databricks_schema="test_schema",
        )

    @pytest.fixture
    def sample_file_data(self):
        """Create sample FileData for testing."""
        return FileData(
            identifier="test-file-id",
            connector_type="test_databricks_volumes",
            source_identifiers=SourceIdentifiers(
                filename="test.txt", fullpath="test/path/test.txt", relative_path="test/test.txt"
            ),
            additional_metadata={
                "catalog": "test_catalog",
                "path": "/Volumes/test_catalog/default/test_volume/test/path/test.txt",
            },
            metadata=FileDataSourceMetadata(
                url="/Volumes/test_catalog/default/test_volume/test/path/test.txt",
                date_modified="12345",
            ),
            display_name="test/path/test.txt",
        )

    @patch("unstructured_ingest.processes.utils.logging.connector.logger")
    def test_indexer_logging(
        self, mock_logger, mock_workspace_client, connection_config, indexer_config
    ):
        """Test that the indexer logs operations correctly."""
        # Mock the DBFS list response
        mock_file_info = MagicMock()
        mock_file_info.is_dir = False
        mock_file_info.path = "/Volumes/test_catalog/test_schema/test_volume/test_path/test.txt"
        mock_file_info.modification_time = 1234567890

        mock_workspace_client.dbfs.list.return_value = [mock_file_info] * 6  # More than 5 files

        # Mock the get_client method to return our mock client
        with mock.patch.object(
            DatabricksVolumesConnectionConfig, "get_client", return_value=mock_workspace_client
        ):
            indexer = TestDatabricksVolumesIndexer(
                connection_config=connection_config, index_config=indexer_config
            )

            # Test precheck logging
            indexer.precheck()

            # Verify connection validation logs
            mock_logger.info.assert_any_call("Starting %s", "Connection validation")
            mock_logger.debug.assert_any_call(
                "Connection to %s validated successfully", "test_databricks_volumes"
            )

            # Reset logger mock to test run method
            mock_logger.reset_mock()

            # Test run method logging
            file_data_list = list(indexer.run())

            # Verify indexing start log
            mock_logger.info.assert_any_call(
                "Starting indexing of %s", "test_databricks_volumes files"
            )

            # Verify file indexing operation start
            mock_logger.info.assert_any_call("Starting %s", "File indexing")

            # Verify indexing complete log
            mock_logger.info.assert_any_call(
                "Indexing completed: %s %s items indexed", 6, "test_databricks_volumes files"
            )

            # Verify we got the expected number of file data objects
            assert len(file_data_list) == 6

    @patch("unstructured_ingest.processes.utils.logging.connector.logger")
    def test_indexer_error_logging(
        self, mock_logger, mock_workspace_client, connection_config, indexer_config
    ):
        """Test that indexer logs errors correctly."""
        # Mock a connection error
        mock_workspace_client.dbfs.list.side_effect = Exception("Connection failed")

        with mock.patch.object(
            DatabricksVolumesConnectionConfig, "get_client", return_value=mock_workspace_client
        ):
            indexer = TestDatabricksVolumesIndexer(
                connection_config=connection_config, index_config=indexer_config
            )

            # Test that error is logged and exception is raised
            with pytest.raises(Exception):
                list(indexer.run())

            # Verify error logging
            mock_logger.error.assert_called()

    @patch("unstructured_ingest.processes.utils.logging.connector.logger")
    def test_downloader_logging(
        self,
        mock_logger,
        mock_workspace_client,
        connection_config,
        downloader_config,
        sample_file_data,
    ):
        """Test that the downloader logs operations correctly."""
        # Mock the download response
        mock_download_context = MagicMock()
        mock_download_context._read_handle.read.return_value = b"test file content"
        mock_workspace_client.dbfs.download.return_value.__enter__.return_value = (
            mock_download_context
        )

        with mock.patch.object(
            DatabricksVolumesConnectionConfig, "get_client", return_value=mock_workspace_client
        ):
            downloader = TestDatabricksVolumesDownloader(
                connection_config=connection_config, download_config=downloader_config
            )

            # Test precheck logging
            downloader.precheck()

            # Verify connection validation logs
            mock_logger.info.assert_any_call("Starting %s", "Connection validation")
            mock_logger.debug.assert_any_call(
                "Connection to %s validated successfully", "test_databricks_volumes"
            )

            # Reset logger mock to test run method
            mock_logger.reset_mock()

            # Test download logging
            result = downloader.run(sample_file_data)

            # Verify download start log
            mock_logger.info.assert_any_call("Starting document download")

            # Verify download complete log
            mock_logger.info.assert_any_call("Document download completed")

            assert result is not None

    @patch("unstructured_ingest.processes.utils.logging.connector.logger")
    def test_downloader_error_logging(
        self,
        mock_logger,
        mock_workspace_client,
        connection_config,
        downloader_config,
        sample_file_data,
    ):
        """Test that downloader logs errors correctly."""
        # Mock a download error
        mock_workspace_client.dbfs.download.side_effect = Exception("Download failed")

        with mock.patch.object(
            DatabricksVolumesConnectionConfig, "get_client", return_value=mock_workspace_client
        ):
            downloader = TestDatabricksVolumesDownloader(
                connection_config=connection_config, download_config=downloader_config
            )

            # Test that error is logged and exception is raised
            with pytest.raises(Exception):
                downloader.run(sample_file_data)

            # Verify error logging
            mock_logger.error.assert_called()

    @patch("unstructured_ingest.processes.utils.logging.connector.logger")
    def test_uploader_logging(
        self,
        mock_logger,
        mock_workspace_client,
        connection_config,
        uploader_config,
        sample_file_data,
        tmp_path,
    ):
        """Test that the uploader logs operations correctly."""
        # Create a test file to upload
        test_file = tmp_path / "test.json"
        test_file.write_text(json.dumps([{"type": "Text", "text": "test content"}]))

        # Mock current user
        mock_user = MagicMock()
        mock_user.active = True
        mock_workspace_client.current_user.me.return_value = mock_user

        with mock.patch.object(
            DatabricksVolumesConnectionConfig, "get_client", return_value=mock_workspace_client
        ):
            uploader = TestDatabricksVolumesUploader(
                connection_config=connection_config, upload_config=uploader_config
            )

            # Test precheck logging
            uploader.precheck()

            # Verify connection validation logs
            mock_logger.info.assert_any_call("Starting %s", "Connection validation")
            mock_logger.debug.assert_any_call(
                "Connection to %s validated successfully", "test_databricks_volumes"
            )

            # Reset logger mock to test run method
            mock_logger.reset_mock()

            # Test upload logging
            uploader.run(test_file, sample_file_data)

            # Verify upload start log
            mock_logger.info.assert_any_call("Starting file upload")

            # Verify upload complete log
            mock_logger.info.assert_any_call("File upload completed")

    @patch("unstructured_ingest.processes.utils.logging.connector.logger")
    def test_uploader_error_logging(
        self,
        mock_logger,
        mock_workspace_client,
        connection_config,
        uploader_config,
        sample_file_data,
        tmp_path,
    ):
        """Test that uploader logs errors correctly."""
        # Create a test file to upload
        test_file = tmp_path / "test.json"
        test_file.write_text(json.dumps([{"type": "Text", "text": "test content"}]))

        # Mock current user
        mock_user = MagicMock()
        mock_user.active = True
        mock_workspace_client.current_user.me.return_value = mock_user

        # Mock upload error
        mock_workspace_client.files.upload.side_effect = Exception("Upload failed")

        with mock.patch.object(
            DatabricksVolumesConnectionConfig, "get_client", return_value=mock_workspace_client
        ):
            uploader = TestDatabricksVolumesUploader(
                connection_config=connection_config, upload_config=uploader_config
            )

            # Test that error is logged and exception is raised
            with pytest.raises(Exception):
                uploader.run(test_file, sample_file_data)

            # Verify error logging
            mock_logger.error.assert_called()

    @patch("unstructured_ingest.processes.utils.logging.connector.logger")
    def test_connection_validation_failure_logging(
        self, mock_logger, mock_workspace_client, connection_config, indexer_config
    ):
        """Test that connection validation failures are logged correctly."""
        # Mock a connection error during client creation
        with mock.patch.object(
            DatabricksVolumesConnectionConfig,
            "get_client",
            side_effect=Exception("Connection failed"),
        ):
            indexer = TestDatabricksVolumesIndexer(
                connection_config=connection_config, index_config=indexer_config
            )

            with pytest.raises(Exception):
                indexer.precheck()

            # Verify connection failure is logged
            mock_logger.error.assert_called()
            # Should contain "Failed to validate" in one of the calls
            calls = [str(call) for call in mock_logger.error.call_args_list]
            assert any("Failed to validate" in call for call in calls)

    @patch("unstructured_ingest.processes.utils.logging.connector.logger")
    def test_logging_with_context(
        self, mock_logger, mock_workspace_client, connection_config, indexer_config
    ):
        """Test that operations are logged with proper context information."""
        mock_workspace_client.dbfs.list.return_value = []

        with mock.patch.object(
            DatabricksVolumesConnectionConfig, "get_client", return_value=mock_workspace_client
        ):
            indexer = TestDatabricksVolumesIndexer(
                connection_config=connection_config, index_config=indexer_config
            )

            # Test precheck logs with context
            indexer.precheck()

            # Verify that debug logs contain context information
            debug_calls = [
                call.args for call in mock_logger.debug.call_args_list if len(call.args) > 1
            ]

            # Should have parameters logged
            param_calls = [
                call for call in debug_calls if len(call) >= 2 and "parameters" in str(call[0])
            ]
            assert len(param_calls) > 0, "Should log operation parameters"
