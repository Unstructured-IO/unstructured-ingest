"""Vector destinations must not stage elements that have no embedding.

Elements with empty text are skipped by the embedder and arrive at the stager without
an "embeddings" key. Qdrant, Chroma and Astra DB each map that key onto a vector field
and cannot use the element without it, so they drop it before upload.
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


def _stagers():
    return [
        pytest.param(CloudQdrantUploadStager(CloudQdrantUploadStagerConfig()), id="qdrant"),
        pytest.param(ChromaUploadStager(ChromaUploadStagerConfig()), id="chroma"),
        pytest.param(AstraDBUploadStager(AstraDBUploadStagerConfig()), id="astradb"),
    ]


@pytest.fixture
def file_data():
    return FileData(
        connector_type="test",
        identifier="test_record_id",
        source_identifiers=SourceIdentifiers(filename="doc.pdf", fullpath="doc.pdf"),
    )


@pytest.mark.parametrize("stager", _stagers())
def test_includes_element_with_embeddings(stager):
    assert stager.should_include(element_dict=dict(EMBEDDED)) is True


@pytest.mark.parametrize("stager", _stagers())
def test_excludes_element_without_embeddings(stager):
    assert stager.should_include(element_dict=dict(UNEMBEDDED)) is False


@pytest.mark.parametrize("stager", _stagers())
@pytest.mark.parametrize("suffix", [".json", ".ndjson"])
def test_run_drops_unembedded_elements(stager, suffix, file_data, tmp_path: Path):
    input_file = tmp_path / f"elements{suffix}"
    elements = [dict(EMBEDDED), dict(UNEMBEDDED)]
    if suffix == ".json":
        input_file.write_text(json.dumps(elements))
    else:
        with input_file.open("w") as f:
            ndjson.dump(elements, f)

    output_path = stager.run(
        elements_filepath=input_file,
        file_data=file_data,
        output_dir=tmp_path / "staged",
        output_filename=f"elements{suffix}",
    )

    if suffix == ".json":
        staged = json.loads(output_path.read_text())
    else:
        with output_path.open() as f:
            staged = ndjson.load(f)
    assert len(staged) == 1


def test_astradb_keeps_unembedded_when_astra_generates_vectors():
    """Astra vectorizes from the content itself, so the element is still usable."""
    stager = AstraDBUploadStager(
        upload_stager_config=AstraDBUploadStagerConfig(astra_generated_embeddings=True)
    )
    assert stager.should_include(element_dict=dict(UNEMBEDDED)) is True
