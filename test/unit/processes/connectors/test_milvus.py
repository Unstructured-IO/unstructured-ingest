import json
from pathlib import Path

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.milvus import (
    CONNECTOR_TYPE,
    MilvusUploadStager,
)
from unstructured_ingest.utils import ndjson


@pytest.fixture
def file_data():
    return FileData(
        connector_type=CONNECTOR_TYPE,
        identifier="milvus_test_id",
        source_identifiers=SourceIdentifiers(filename="test.json", fullpath="test.json"),
    )


@pytest.fixture
def stager() -> MilvusUploadStager:
    return MilvusUploadStager()


def _mixed_elements() -> list[dict]:
    """Mix of elements: some with embeddings (non-empty text) and some without
    (empty text, like the page-boundary UncategorizedText elements from the bug
    report)."""
    return [
        {
            "element_id": "e1",
            "text": "first chunk",
            "embeddings": [0.1] * 8,
            "metadata": {"filename": "doc.pdf", "page_number": 1},
        },
        {
            "element_id": "e2",
            "text": "",
            "metadata": {"filename": "doc.pdf", "page_number": 2},
        },
        {
            "element_id": "e3",
            "text": "third chunk",
            "embeddings": [0.3] * 8,
            "metadata": {"filename": "doc.pdf", "page_number": 3},
        },
        {
            "element_id": "e4",
            "text": "",
            "metadata": {"filename": "doc.pdf", "page_number": 4},
        },
    ]


def test_should_include_true_when_embeddings_present(stager: MilvusUploadStager):
    element = {"text": "hello", "embeddings": [0.1, 0.2, 0.3]}
    assert stager.should_include(element_dict=element) is True


def test_should_include_false_when_embeddings_missing(stager: MilvusUploadStager):
    element = {"text": "", "metadata": {"page_number": 2}}
    assert stager.should_include(element_dict=element) is False


def test_should_include_true_when_embeddings_is_empty_list(stager: MilvusUploadStager):
    """Presence of the key is what matters here; value-level concerns (dim
    mismatch, etc.) are Milvus' responsibility to surface."""
    element = {"text": "anything", "embeddings": []}
    assert stager.should_include(element_dict=element) is True


def test_should_include_true_when_embeddings_is_none(stager: MilvusUploadStager):
    """Same rationale: the predicate checks key presence, not value validity."""
    element = {"text": "anything", "embeddings": None}
    assert stager.should_include(element_dict=element) is True


def test_run_drops_elements_without_embeddings_json(
    stager: MilvusUploadStager, file_data: FileData, tmp_path: Path
):
    input_file = tmp_path / "elements.json"
    input_file.write_text(json.dumps(_mixed_elements()))

    output_path = stager.run(
        elements_filepath=input_file,
        file_data=file_data,
        output_dir=tmp_path / "staged",
        output_filename="elements.json",
    )

    staged = json.loads(output_path.read_text())
    assert [e["element_id"] for e in staged] == ["e1", "e3"]
    assert all("embeddings" in e for e in staged)


def test_run_drops_elements_without_embeddings_ndjson(
    stager: MilvusUploadStager, file_data: FileData, tmp_path: Path
):
    input_file = tmp_path / "elements.ndjson"
    with input_file.open("w") as f:
        ndjson.dump(_mixed_elements(), f)

    output_path = stager.run(
        elements_filepath=input_file,
        file_data=file_data,
        output_dir=tmp_path / "staged",
        output_filename="elements.ndjson",
    )

    with output_path.open() as f:
        staged = ndjson.load(f)
    assert [e["element_id"] for e in staged] == ["e1", "e3"]
    assert all("embeddings" in e for e in staged)


def test_run_keeps_all_when_every_element_has_embeddings(
    stager: MilvusUploadStager, file_data: FileData, tmp_path: Path
):
    elements = [
        {"element_id": f"e{i}", "text": f"chunk {i}", "embeddings": [0.1] * 4} for i in range(5)
    ]
    input_file = tmp_path / "elements.json"
    input_file.write_text(json.dumps(elements))

    output_path = stager.run(
        elements_filepath=input_file,
        file_data=file_data,
        output_dir=tmp_path / "staged",
        output_filename="elements.json",
    )

    staged = json.loads(output_path.read_text())
    assert len(staged) == len(elements)


def test_run_produces_empty_output_when_no_embeddings(
    stager: MilvusUploadStager, file_data: FileData, tmp_path: Path
):
    elements = [
        {"element_id": "e1", "text": ""},
        {"element_id": "e2", "text": ""},
    ]
    input_file = tmp_path / "elements.json"
    input_file.write_text(json.dumps(elements))

    output_path = stager.run(
        elements_filepath=input_file,
        file_data=file_data,
        output_dir=tmp_path / "staged",
        output_filename="elements.json",
    )

    assert json.loads(output_path.read_text()) == []
