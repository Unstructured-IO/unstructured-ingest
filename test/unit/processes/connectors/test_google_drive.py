import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.google_drive import CONNECTOR_TYPE, _get_extension


@pytest.mark.parametrize(
    ("mime_type", "expected_extension"),
    [
        ("application/vnd.google-apps.document", ".docx"),
        ("application/vnd.google-apps.spreadsheet", ".xlsx"),
        ("application/vnd.google-apps.presentation", ".pptx"),
    ],
)
def test_get_extension_for_google_workspace_files(mime_type: str, expected_extension: str):
    file_data = FileData(
        connector_type=CONNECTOR_TYPE,
        identifier="test-id",
        source_identifiers=SourceIdentifiers(filename="file", fullpath="file"),
        additional_metadata={"mimeType": mime_type},
    )
    assert _get_extension(file_data) == expected_extension


def test_get_extension_returns_empty_for_unknown_mime_type():
    file_data = FileData(
        connector_type=CONNECTOR_TYPE,
        identifier="test-id",
        source_identifiers=SourceIdentifiers(filename="file", fullpath="file"),
        additional_metadata={"mimeType": "application/pdf"},
    )
    assert _get_extension(file_data) == ""
