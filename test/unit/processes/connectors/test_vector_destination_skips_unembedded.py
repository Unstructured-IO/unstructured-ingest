"""Vector destinations must not stage elements the embedder intentionally skipped.

The embedder embeds only elements with text, so an element with empty text arrives at
the stager with no "embeddings" key and nothing to index. Qdrant, Chroma and Astra DB
each map that key onto a vector field, so they drop those elements.

An element that has text but no embedding is a different case: it means no embedder ran
at all. Those are kept, so each destination's existing failure still surfaces instead of
a workflow silently uploading nothing.
"""

import json
from pathlib import Path

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.astradb import (
    AstraDBUploadStager,
    AstraDBUploadStagerConfig,
)
from unstructured_ingest.processes.connectors.chroma import (
    ChromaUploadStager,
    ChromaUploadStagerConfig,
)
from unstructured_ingest.processes.connectors.qdrant.cloud import (
    CloudQdrantUploadStager,
    CloudQdrantUploadStagerConfig,
)
from unstructured_ingest.utils import ndjson

EMBEDDED = {
    "element_id": "embedded",
    "type": "NarrativeText",
    "text": "hello world",
    "embeddings": [0.1, 0.2, 0.3, 0.4],
    "metadata": {"filename": "doc.pdf", "page_number": 1},
}
UNEMBEDDED = {
    "element_id": "unembedded",
    "type": "UncategorizedText",
    "text": "",
    "metadata": {"filename": "doc.pdf", "page_number": 2},
}
# text present, no embedding: what every element looks like when no embedder ran
NO_EMBEDDER = {
    "element_id": "no_embedder",
    "type": "NarrativeText",
    "text": "hello world",
    "metadata": {"filename": "doc.pdf", "page_number": 3},
}


def _stagers():
    """(stager, staged_text) per destination, so a test can identify which element survived."""
    return [
        pytest.param(
            CloudQdrantUploadStager(CloudQdrantUploadStagerConfig()),
            lambda row: row["payload"]["text"],
            id="qdrant",
        ),
        pytest.param(
            ChromaUploadStager(ChromaUploadStagerConfig()),
            lambda row: row["document"],
            id="chroma",
        ),
        pytest.param(
            AstraDBUploadStager(AstraDBUploadStagerConfig()),
            lambda row: row["content"],
            id="astradb",
        ),
    ]


@pytest.fixture
def file_data():
    return FileData(
        connector_type="test",
        identifier="test_record_id",
        source_identifiers=SourceIdentifiers(filename="doc.pdf", fullpath="doc.pdf"),
    )


def _write(input_file: Path, elements: list[dict]) -> None:
    if input_file.suffix == ".json":
        input_file.write_text(json.dumps(elements))
    else:
        with input_file.open("w") as f:
            ndjson.dump(elements, f)


def _read(output_path: Path) -> list[dict]:
    if output_path.suffix == ".json":
        return json.loads(output_path.read_text())
    with output_path.open() as f:
        return ndjson.load(f)


def _stage(stager, elements, file_data, tmp_path: Path, suffix: str) -> list[dict]:
    input_file = tmp_path / f"elements{suffix}"
    _write(input_file, elements)
    output_path = stager.run(
        elements_filepath=input_file,
        file_data=file_data,
        output_dir=tmp_path / "staged",
        output_filename=f"elements{suffix}",
    )
    return _read(output_path)


@pytest.mark.parametrize("stager,staged_text", _stagers())
def test_includes_element_with_embeddings(stager, staged_text):
    assert stager.should_include(element_dict=dict(EMBEDDED)) is True


@pytest.mark.parametrize("stager,staged_text", _stagers())
def test_excludes_element_with_no_text_and_no_embeddings(stager, staged_text):
    assert stager.should_include(element_dict=dict(UNEMBEDDED)) is False


@pytest.mark.parametrize("stager,staged_text", _stagers())
def test_includes_element_with_text_but_no_embeddings(stager, staged_text):
    """No embedder ran; the destination's own error must still surface."""
    assert stager.should_include(element_dict=dict(NO_EMBEDDER)) is True


@pytest.mark.parametrize("stager,staged_text", _stagers())
@pytest.mark.parametrize("suffix", [".json", ".ndjson"])
def test_run_drops_only_the_unembedded_element(stager, staged_text, suffix, file_data, tmp_path):
    staged = _stage(stager, [dict(EMBEDDED), dict(UNEMBEDDED)], file_data, tmp_path, suffix)

    assert len(staged) == 1
    assert staged_text(staged[0]) == EMBEDDED["text"]


@pytest.mark.parametrize("suffix", [".json", ".ndjson"])
def test_run_keeps_unembedded_when_astra_generates_vectors(suffix, file_data, tmp_path):
    """Astra vectorizes from the content, so nothing is dropped in that mode.

    Elements carry no embeddings at all here, since supplying both is rejected.
    """
    stager = AstraDBUploadStager(
        upload_stager_config=AstraDBUploadStagerConfig(astra_generated_embeddings=True)
    )
    staged = _stage(stager, [dict(NO_EMBEDDER), dict(UNEMBEDDED)], file_data, tmp_path, suffix)

    assert len(staged) == 2
    assert [row["$vectorize"] for row in staged] == [NO_EMBEDDER["text"], UNEMBEDDED["text"]]


def test_astradb_still_raises_when_no_embedder_ran(file_data, tmp_path):
    """The 'No vectors provided' configuration error must not be filtered away."""
    stager = AstraDBUploadStager(upload_stager_config=AstraDBUploadStagerConfig())

    with pytest.raises(ValueError, match="No vectors provided"):
        _stage(stager, [dict(NO_EMBEDDER)], file_data, tmp_path, ".json")


def test_astradb_empty_embeddings_still_raises(file_data, tmp_path):
    """An empty vector is an invalid value, not an element to drop silently."""
    stager = AstraDBUploadStager(upload_stager_config=AstraDBUploadStagerConfig())

    with pytest.raises(ValueError, match="No vectors provided"):
        _stage(stager, [dict(EMBEDDED, embeddings=[])], file_data, tmp_path, ".json")
