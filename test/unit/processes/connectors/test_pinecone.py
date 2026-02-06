import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import (
    NotFoundError,
    ProviderError,
    RateLimitError,
    UserAuthError,
    WriteError,
)
from unstructured_ingest.processes.connectors.pinecone import (
    CONNECTOR_TYPE,
    PineconeUploader,
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


class _FakePineconeApiException(Exception):
    """Minimal stand-in for pinecone.exceptions.PineconeApiException."""

    def __init__(self, status):
        self.status = status
        super().__init__(f"Pinecone API error with status {status}")


@pytest.mark.parametrize(
    ("status", "expected_error_type"),
    [
        (401, UserAuthError),
        (403, UserAuthError),
        (404, NotFoundError),
        (429, RateLimitError),
        (400, WriteError),
        (409, WriteError),
        (422, WriteError),
        (500, ProviderError),
        (502, ProviderError),
        (503, ProviderError),
        (None, WriteError),
    ],
)
def test_map_pinecone_api_error_maps_status_codes(status, expected_error_type):
    api_error = _FakePineconeApiException(status=status)
    wrapped = PineconeUploader._map_pinecone_api_error(api_error)
    assert isinstance(wrapped, expected_error_type), (
        f"Expected {expected_error_type.__name__} for status {status}, got {type(wrapped).__name__}"
    )
    assert "Pinecone API error:" in str(wrapped)


def test_map_pinecone_api_error_preserves_cause():
    """Verify that the original exception is chained via __cause__ when raised."""
    api_error = _FakePineconeApiException(status=500)
    wrapped = PineconeUploader._map_pinecone_api_error(api_error)
    try:
        raise wrapped from api_error
    except ProviderError as caught:
        assert caught.__cause__ is api_error
