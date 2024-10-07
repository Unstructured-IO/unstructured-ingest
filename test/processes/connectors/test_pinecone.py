import pytest

from unstructured_ingest.v2.processes.connectors.pinecone import (
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
    if metadata_fields is not None:
        stager = PineconeUploadStager(
            upload_stager_config=PineconeUploadStagerConfig(metadata_fields=metadata_fields)
        )
    else:
        stager = PineconeUploadStager()
    results = stager.conform_dict(test_element_dict.copy())
    results.pop("id")
    assert test_element_dict["embeddings"] == results.pop("values")

    assert all(
        results["metadata"][key] == test_element_dict["metadata"][key] for key in expected_to_exist
    )
    assert all(key not in results["metadata"] for key in not_expected_to_exist)
