import pytest

from unstructured_ingest.v2.processes.connectors.milvus import (
    MilvusUploadStager,
    MilvusUploadStagerConfig,
)


@pytest.mark.parametrize(
    ("given_element", "given_field_include_list", "then_element", "then_error"),
    [
        ({"x": "y"}, None, {"x": "y", "is_continuation": False}, False),
        (
            {
                "type": "UncategorizedText",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
            },
            ["type", "element_id"],
            {
                "type": "UncategorizedText",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
            },
            False,
        ),
        (
            {
                "type": "UncategorizedText",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
                "parent_id": "qwerty",
            },
            ["type", "element_id"],
            {
                "type": "UncategorizedText",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
            },
            False,
        ),
        (
            {
                "type": "UncategorizedText",
                "parent_id": "qwerty",
            },
            ["type", "element_id"],
            {},
            True,
        ),
    ],
)
def test_milvus_stager_processes_elements_correctly(
    given_element, given_field_include_list, then_element, then_error
):
    config = MilvusUploadStagerConfig(fields_to_include=given_field_include_list)
    stager = MilvusUploadStager(upload_stager_config=config)

    if then_error:
        with pytest.raises(KeyError):
            stager.conform_dict(data=given_element)
    else:
        stager.conform_dict(data=given_element)
        assert given_element == then_element


@pytest.mark.parametrize(
    (
        "given_flatten_metadata",
        "given_element",
        "then_element",
    ),
    [
        (
            True,
            {
                "type": "UncategorizedText",
                "metadata": {
                    "filename": "fake-memo.pdf",
                },
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
            },
            {
                "type": "UncategorizedText",
                "filename": "fake-memo.pdf",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
                "is_continuation": False,
            },
        ),
        (
            False,
            {
                "type": "UncategorizedText",
                "metadata": {
                    "filename": "fake-memo.pdf",
                },
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
            },
            {
                "type": "UncategorizedText",
                "metadata": {
                    "filename": "fake-memo.pdf",
                },
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
                "is_continuation": False,
            },
        ),
    ],
)
def test_milvus_stager_processes_metadata_correctly(
    given_flatten_metadata, given_element, then_element
):
    config = MilvusUploadStagerConfig(flatten_metadata=given_flatten_metadata)
    stager = MilvusUploadStager(upload_stager_config=config)

    stager.conform_dict(data=given_element)
    assert given_element == then_element


@pytest.mark.parametrize(
    (
        "given_field_include_list",
        "given_element",
        "then_element",
    ),
    [
        (
            ["type", "filename", "element_id"],
            {
                "type": "UncategorizedText",
                "metadata": {
                    "filename": "fake-memo.pdf",
                },
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
            },
            {
                "type": "UncategorizedText",
                "filename": "fake-memo.pdf",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
            },
        ),
        (
            ["type", "element_id"],
            {
                "type": "UncategorizedText",
                "metadata": {
                    "filename": "fake-memo.pdf",
                },
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
            },
            {
                "type": "UncategorizedText",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
            },
        ),
    ],
)
def test_milvus_stager_processes_metadata_correctly_when_using_include_list(
    given_field_include_list, given_element, then_element
):
    config = MilvusUploadStagerConfig(
        flatten_metadata=True, fields_to_include=given_field_include_list
    )
    stager = MilvusUploadStager(upload_stager_config=config)

    stager.conform_dict(data=given_element)
    assert given_element == then_element
