from unittest.mock import MagicMock, patch
from urllib.parse import unquote

import pytest

from unstructured_ingest.v2.processes.connectors.fsspec.fsspec import (
    FsspecAccessConfig,
    FsspecConnectionConfig,
    FsspecIndexer,
    FsspecIndexerConfig,
    Secret,
)


@pytest.mark.parametrize(
    ("remote_url", "expected_path"),
    [
        ("dropbox://da ta", "da ta"),
        ("dropbox://da%20ta", "da ta"),
    ]
)
def test_fsspec_indexer_path_decoding(remote_url, expected_path):
    # Initialize index config with given remote_url
    index_config = FsspecIndexerConfig(remote_url=remote_url)
    # We assume an empty access config just for test
    connection_config = FsspecConnectionConfig(access_config=Secret(FsspecAccessConfig()))

    # After initialization, ensure path_without_protocol matches expected result
    assert (
        index_config.path_without_protocol == expected_path
    ), f"Expected {expected_path}, got {index_config.path_without_protocol}"

    # Create the indexer
    indexer = FsspecIndexer(connection_config=connection_config, index_config=index_config)

    # Now index_config should have our expected path
    full_path = expected_path
    assert (
        indexer.index_config.path_without_protocol == full_path
    ), f"Expected path to be {full_path}, got {indexer.index_config.path_without_protocol}"

    # Mock fsspec filesystem class to verify it's called with the correct path
    with patch("fsspec.get_filesystem_class") as mock_fs_class:
        mock_fs_instance = MagicMock()
        mock_fs_class.return_value = lambda **kwargs: mock_fs_instance

        # Mock fs.ls to return a dummy file
        mock_fs_instance.ls.return_value = [
            {"name": full_path + "/file.txt", "type": "file", "size": 123}
        ]

        files = indexer.get_file_data()

        # Verify that fs.ls was called with the correct decoded path
        mock_fs_instance.ls.assert_called_once_with(full_path, detail=True)

        # Assert that we got the expected file and it has the correct name
        assert len(files) == 1
        assert (
            files[0]["name"] == full_path + "/file.txt"
        ), "File name did not match expected output"


@pytest.mark.parametrize(
    "remote_url",
    [
        "dropbox://da ta",
        "dropbox://da%20ta",
    ],
)
def test_fsspec_indexer_precheck(remote_url):
    # This test ensures that precheck doesn't raise errors and calls head appropriately
    index_config = FsspecIndexerConfig(remote_url=remote_url)
    connection_config = FsspecConnectionConfig(access_config=Secret(FsspecAccessConfig()))
    indexer = FsspecIndexer(connection_config=connection_config, index_config=index_config)

    with patch("fsspec.get_filesystem_class") as mock_fs_class:
        mock_fs_instance = MagicMock()
        mock_fs_class.return_value = lambda **kwargs: mock_fs_instance

        mock_fs_instance.ls.return_value = [
            {
                "name": "/" + unquote(remote_url.split("://", 1)[1]) + "/file.txt",
                "type": "file",
                "size": 123,
            }
        ]

        # head should be called on that file
        mock_fs_instance.head.return_value = {"Content-Length": "123"}

        # If precheck does not raise SourceConnectionError, we consider it passed
        indexer.precheck()

        # Check that fs.head was called with the correct file path
        mock_fs_instance.head.assert_called_once()
