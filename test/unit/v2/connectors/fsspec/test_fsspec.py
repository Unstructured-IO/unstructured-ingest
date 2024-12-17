from pathlib import Path

from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.fsspec.fsspec import (
    FsspecAccessConfig,
    FsspecConnectionConfig,
    FsspecUploader,
    FsspecUploaderConfig,
)


def test_get_upload_path_with_source_identifiers():
    upload_config = FsspecUploaderConfig(remote_url="s3://my-bucket")
    connection_config = FsspecConnectionConfig(access_config=FsspecAccessConfig())
    uploader = FsspecUploader(
        connection_config=connection_config,
        upload_config=upload_config,
    )
    file_data = FileData(
        identifier="123",
        source_identifiers=SourceIdentifiers("path/to/file.txt", "/full/path/to/file.txt"),
        local_download_path=None,
        additional_metadata={},
        connector_type="fsspec",
        display_name="file.txt",
        metadata={},
    )

    expected_path = Path("my-bucket/full/path/to/file.txt.json")
    assert uploader.get_upload_path(file_data) == expected_path


def test_get_upload_path_with_source_identifiers_rel_path():
    upload_config = FsspecUploaderConfig(remote_url="s3://my-bucket")
    connection_config = FsspecConnectionConfig(access_config=FsspecAccessConfig())
    uploader = FsspecUploader(
        connection_config=connection_config,
        upload_config=upload_config,
    )
    file_data = FileData(
        identifier="123",
        source_identifiers=SourceIdentifiers(
            "path/to/file.txt", "/full/path/to/file.txt", "rel/path/to/file.txt"
        ),
        local_download_path=None,
        additional_metadata={},
        connector_type="fsspec",
        display_name="file.txt",
        metadata={},
    )

    expected_path = Path("my-bucket/rel/path/to/file.txt.json")
    assert uploader.get_upload_path(file_data) == expected_path


def test_get_upload_path_with_local_download_path():
    upload_config = FsspecUploaderConfig(remote_url="s3://my-bucket")
    connection_config = FsspecConnectionConfig(access_config=FsspecAccessConfig())
    uploader = FsspecUploader(
        connection_config=connection_config,
        upload_config=upload_config,
    )
    file_data = FileData(
        identifier="123",
        local_download_path="local/path/to/file.txt",
        additional_metadata={},
        connector_type="fsspec",
        display_name="file.txt",
        metadata={},
    )
    expected_path = Path("my-bucket/file.txt.json")
    assert uploader.get_upload_path(file_data) == expected_path


def test_get_upload_path_with_identifier():
    upload_config = FsspecUploaderConfig(remote_url="s3://my-bucket")
    connection_config = FsspecConnectionConfig(access_config=FsspecAccessConfig())
    uploader = FsspecUploader(
        connection_config=connection_config,
        upload_config=upload_config,
    )
    file_data = FileData(
        identifier="123",
        additional_metadata={},
        connector_type="fsspec",
        display_name="file.txt",
        metadata={},
    )
    expected_path = Path("my-bucket/123.json")
    assert uploader.get_upload_path(file_data) == expected_path
