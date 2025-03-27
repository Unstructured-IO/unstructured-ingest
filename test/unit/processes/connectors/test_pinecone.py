import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.pinecone import (
    CONNECTOR_TYPE,
    PineconeUploadStager,
    PineconeUploadStagerConfig,
)


@pytest.fixture
def test_element_dict():
    return {
        "embeddings": [0, 1],
        "text": "test dict",
        "metadata": {
            "text_as_html": "text as html",
            "foo": "foo",
        },
    }


@pytest.mark.parametrize(
    ("metadata_fields", "expected_to_exist", "not_expected_to_exist"),
    [
        (None, ["text_as_html"], ["foo"]),
        (("foo",), ["foo"], ["text_as_html"]),
    ],
)
def test_conform_dict(
    monkeypatch, test_element_dict, metadata_fields, expected_to_exist, not_expected_to_exist
):
    file_data = FileData(
        connector_type=CONNECTOR_TYPE,
        identifier="pinecone_mock_id",
        source_identifiers=SourceIdentifiers(filename="file.txt", fullpath="file.txt"),
    )
    if metadata_fields is not None:
        stager = PineconeUploadStager(
            upload_stager_config=PineconeUploadStagerConfig(metadata_fields=metadata_fields)
        )
    else:
        stager = PineconeUploadStager()
    results = stager.conform_dict(test_element_dict.copy(), file_data=file_data)
    results.pop("id")
    assert test_element_dict["embeddings"] == results.pop("values")

    assert all(
        results["metadata"][key] == test_element_dict["metadata"][key] for key in expected_to_exist
    )
    assert all(key not in results["metadata"] for key in not_expected_to_exist)
