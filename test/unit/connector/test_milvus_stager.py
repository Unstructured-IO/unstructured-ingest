import pytest

from unstructured_ingest.types import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.milvus import (
    CONNECTOR_TYPE,
    MilvusUploadStager,
    MilvusUploadStagerConfig,
)

FILE_DATA = FileData(
    source_identifiers=SourceIdentifiers(fullpath="fake-memo.pdf", filename="fake-memo.pdf"),
    connector_type=CONNECTOR_TYPE,
    identifier="mock file data",
)


@pytest.mark.parametrize(
    ("given_element", "given_field_include_list", "then_element", "then_error"),
    [
        (
            {"x": "y"},
            None,
            {
                "x": "y",
                "is_continuation": False,
                "record_id": "mock file data",
            },
            False,
        ),
        (
            {
                "type": "UncategorizedText",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
            },
            ["type", "element_id"],
            {
                "type": "UncategorizedText",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
                "record_id": "mock file data",
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
                "record_id": "mock file data",
            },
            False,
        ),
        (
            {
                "type": "UncategorizedText",
                "parent_id": "qwerty",
            },
            ["type", "element_id"],
            {
                "record_id": "mock file data",
            },
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
            stager.conform_dict(element_dict=given_element, file_data=FILE_DATA)
    else:
        staged_element = stager.conform_dict(element_dict=given_element, file_data=FILE_DATA)
        assert staged_element == then_element


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
                "record_id": "mock file data",
            },
            {
                "type": "UncategorizedText",
                "filename": "fake-memo.pdf",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
                "is_continuation": False,
                "record_id": "mock file data",
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
                "record_id": "mock file data",
            },
        ),
    ],
)
def test_milvus_stager_processes_metadata_correctly(
    given_flatten_metadata, given_element, then_element
):
    config = MilvusUploadStagerConfig(flatten_metadata=given_flatten_metadata)
    stager = MilvusUploadStager(upload_stager_config=config)

    staged_element = stager.conform_dict(element_dict=given_element, file_data=FILE_DATA)
    assert staged_element == then_element


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
                "record_id": "mock file data",
            },
            {
                "type": "UncategorizedText",
                "filename": "fake-memo.pdf",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
                "record_id": "mock file data",
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
                "record_id": "mock file data",
            },
            {
                "type": "UncategorizedText",
                "element_id": "be34cd2d71310ec72bfef3d1be2b2b36",
                "record_id": "mock file data",
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

    staged_element = stager.conform_dict(element_dict=given_element, file_data=FILE_DATA)
    assert staged_element == then_element
