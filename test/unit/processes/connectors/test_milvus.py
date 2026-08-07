import json
from pathlib import Path

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import (
    DestinationConnectionError,
    UserAuthError,
    UserError,
    WriteError,
)
from unstructured_ingest.processes.connectors.milvus import (
    CONNECTOR_TYPE,
    MilvusUploadStager,
    _classify_milvus_exception,
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


# ---------------------------------------------------------------------------
# Error classification (PLU-543)
#
# Customer-caused Milvus failures (bad/expired creds, permission, bad request,
# missing resource) must surface as client errors (UserAuthError/UserError), not
# platform errors (WriteError/DestinationConnectionError) that burn the platform
# Job Completions SLO. When a gRPC call fails at the transport level, pymilvus
# re-raises MilvusException(e.code(), ...) where .code is the grpc.StatusCode
# enum member, so that is what we classify on.
# ---------------------------------------------------------------------------


def _milvus_exception(status_code):
    from pymilvus import MilvusException

    return MilvusException(code=status_code, message=f"boom: {status_code}")


@pytest.mark.parametrize(
    "status_name",
    ["PERMISSION_DENIED", "INVALID_ARGUMENT", "NOT_FOUND"],
)
def test_classify_milvus_exception_user_error(status_name: str):
    import grpc

    exc = _milvus_exception(getattr(grpc.StatusCode, status_name))
    platform_fallback = WriteError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    # Reclassified to the client-error class, not the platform fallback.
    assert isinstance(result, UserError)
    assert not isinstance(result, UserAuthError)
    assert result is not platform_fallback


def test_classify_milvus_exception_unauthenticated_is_auth_error():
    import grpc

    exc = _milvus_exception(grpc.StatusCode.UNAUTHENTICATED)
    platform_fallback = WriteError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    # UNAUTHENTICATED (401) is the org-315209346834 signature: it must be the
    # auth subclass, which is still a UserError (client-class), never platform.
    assert isinstance(result, UserAuthError)
    assert isinstance(result, UserError)
    assert result is not platform_fallback


def test_classify_milvus_exception_unrelated_code_falls_through_to_platform():
    import grpc

    # An unrelated/server-side status (not in the reclassified set) must stay a
    # platform error so genuine platform failures still count against the SLO.
    exc = _milvus_exception(grpc.StatusCode.INTERNAL)
    platform_fallback = WriteError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    assert result is platform_fallback
    assert isinstance(result, WriteError)
    assert not isinstance(result, UserError)


def test_classify_milvus_exception_precheck_fallback_is_destination_error():
    import grpc

    # Same fall-through behavior with precheck's platform class.
    exc = _milvus_exception(grpc.StatusCode.INTERNAL)
    platform_fallback = DestinationConnectionError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    assert result is platform_fallback
    assert isinstance(result, DestinationConnectionError)


def test_classify_milvus_exception_server_side_int_code_falls_through():
    # A server-side business status carries an int Milvus ErrorCode in .code
    # (not a grpc.StatusCode), so it must not be reclassified as a client error.
    from pymilvus import MilvusException

    exc = MilvusException(code=1, message="unexpected server error")
    platform_fallback = WriteError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    assert result is platform_fallback
