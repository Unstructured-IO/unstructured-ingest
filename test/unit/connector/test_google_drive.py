from unstructured_ingest.types import SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.google_drive import GoogleDriveIndexer


def test_map_file_data():
    indexer = GoogleDriveIndexer(connection_config=None, index_config=None)

    file_record = {
        "id": "file_id_123",
        "name": "test_file.txt",
        "webContentLink": "http://example.com/test_file.txt",
        "version": "1",
        "permissions": [{"role": "owner", "type": "user"}],
        "createdTime": "2023-01-01T00:00:00Z",
        "modifiedTime": "2023-01-02T00:00:00Z",
        "parent_path": "/parent_folder",
        "parent_root_path": "/parent_folder",
    }

    file_data = indexer.map_file_data(file_record)

    assert file_data.connector_type == "google_drive"
    assert file_data.identifier == "file_id_123"
    assert file_data.source_identifiers == SourceIdentifiers(
        filename="test_file.txt", fullpath="/parent_folder/test_file.txt", rel_path="test_file.txt"
    )
    assert file_data.metadata.url == "http://example.com/test_file.txt"
    assert file_data.metadata.version == "1"
    assert file_data.metadata.permissions_data == [{"role": "owner", "type": "user"}]
    assert file_data.metadata.record_locator == {"file_id": "file_id_123"}
