import json
import traceback
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import grpc
import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import (
    DestinationConnectionError,
    UnstructuredIngestError,
    UserAuthError,
    UserError,
    WriteError,
)
from unstructured_ingest.processes.connectors.milvus import (
    CONNECTOR_TYPE,
    MilvusConnectionConfig,
    MilvusUploader,
    MilvusUploaderConfig,
    MilvusUploadStager,
    _classify_milvus_exception,
    _grpc_status_code,
    _reclassify_milvus_errors,
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
# Job Completions SLO.
#
# CRUCIAL wire reality (verified against pymilvus 2.6.9 decorators.py):
#   * Status codes in IGNORE_RETRY_CODES -- UNAUTHENTICATED, PERMISSION_DENIED,
#     INVALID_ARGUMENT (+ DEADLINE_EXCEEDED/ALREADY_EXISTS/RESOURCE_EXHAUSTED/
#     UNIMPLEMENTED) -- are re-raised as the RAW grpc.RpcError ("raise e from e"),
#     NEVER wrapped in a MilvusException. So the whole customer-credential set
#     (esp. UNAUTHENTICATED) reaches the connector as a grpc.RpcError.
#   * Other codes (e.g. NOT_FOUND) are retried and, on exhaustion, wrapped into
#     MilvusException(e.code, ...).
# The tests below drive REAL grpc.RpcError subclasses whose .code() returns the
# target grpc.StatusCode -- exactly what pymilvus re-raises -- plus a
# MilvusException-wrapped path.
# ---------------------------------------------------------------------------


class _FakeRpcError(grpc.RpcError):
    """A real grpc.RpcError subclass, shaped like what pymilvus re-raises.

    ``grpc.RpcError`` exposes its status via a ``.code()`` *method* returning a
    ``grpc.StatusCode``; the unwrapped codes (UNAUTHENTICATED, ...) arrive at
    the connector as exactly this.
    """

    def __init__(self, status_code: grpc.StatusCode):
        self._status_code = status_code

    def code(self) -> grpc.StatusCode:
        return self._status_code

    def details(self) -> str:
        return f"synthetic {self._status_code}"


def _milvus_exception(status_code):
    """A MilvusException carrying a grpc.StatusCode -- the pymilvus retry-storm
    wrapped path for codes NOT in IGNORE_RETRY_CODES (e.g. NOT_FOUND)."""
    from pymilvus import MilvusException

    return MilvusException(code=status_code, message=f"boom: {status_code}")


# --- direct classifier tests on the REAL grpc.RpcError type -----------------


@pytest.mark.parametrize(
    "status_name",
    ["PERMISSION_DENIED", "INVALID_ARGUMENT", "NOT_FOUND"],
)
def test_classify_raw_grpc_rpc_error_is_user_error(status_name: str):
    exc = _FakeRpcError(getattr(grpc.StatusCode, status_name))
    platform_fallback = WriteError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    # Reclassified to the client-error class, not the platform fallback.
    assert isinstance(result, UserError)
    assert not isinstance(result, UserAuthError)
    assert result is not platform_fallback


def test_classify_raw_grpc_unauthenticated_is_auth_error():
    # UNAUTHENTICATED is the customer-credential signature and pymilvus 2.6.9
    # never wraps it: it arrives as a RAW grpc.RpcError. It must become the auth
    # subclass (401), which is still a UserError (client-class), never platform.
    exc = _FakeRpcError(grpc.StatusCode.UNAUTHENTICATED)
    platform_fallback = WriteError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    assert isinstance(result, UserAuthError)
    assert isinstance(result, UserError)
    assert result is not platform_fallback


def test_classify_raw_grpc_unrelated_code_falls_through_to_platform():
    # An unrelated/server-side status (not in the reclassified set) must stay a
    # platform error so genuine platform failures still count against the SLO.
    exc = _FakeRpcError(grpc.StatusCode.INTERNAL)
    platform_fallback = WriteError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    assert result is platform_fallback
    assert isinstance(result, WriteError)
    assert not isinstance(result, UserError)


def test_classify_precheck_fallback_is_destination_error():
    # Same fall-through behavior with precheck's platform class.
    exc = _FakeRpcError(grpc.StatusCode.INTERNAL)
    platform_fallback = DestinationConnectionError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    assert result is platform_fallback
    assert isinstance(result, DestinationConnectionError)


def test_reclassify_does_not_chain_raw_provider_exception():
    # Redaction invariant (CHANGELOG 1.6.31 / 1.7.8): the reclassified error must
    # not carry the raw provider exception via __cause__, or its unredacted text
    # would resurface through traceback logging (logger.exception / exc_info).
    secret = "password=hunter2"
    raw = _FakeRpcError(grpc.StatusCode.UNAUTHENTICATED)
    raw.args = (secret,)

    with (
        pytest.raises(UserAuthError) as excinfo,
        _reclassify_milvus_errors(lambda exc: WriteError("redacted fallback")),
    ):
        raise raw

    assert excinfo.value.__cause__ is None
    rendered = "".join(
        traceback.format_exception(type(excinfo.value), excinfo.value, excinfo.value.__traceback__)
    )
    assert secret not in rendered


# --- MilvusException-wrapped path (codes NOT in IGNORE_RETRY_CODES) ----------


def test_classify_milvus_exception_wrapped_grpc_code_is_user_error():
    # NOT_FOUND is not in IGNORE_RETRY_CODES, so after a retry storm it arrives
    # wrapped in a MilvusException carrying the grpc.StatusCode. Still a client
    # error.
    exc = _milvus_exception(grpc.StatusCode.NOT_FOUND)
    platform_fallback = WriteError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    assert isinstance(result, UserError)
    assert result is not platform_fallback


def test_classify_milvus_exception_callable_code_retry_storm_path():
    # The pymilvus 2.6.9 SYNC retry-storm path stuffs the raw grpc.RpcError.code
    # *method* into MilvusException.code (decorators.py:297 uses `e.code` not
    # `e.code()`). _grpc_status_code must resolve the callable.
    from pymilvus import MilvusException

    raw = _FakeRpcError(grpc.StatusCode.NOT_FOUND)
    exc = MilvusException(code=raw.code, message="retry storm")

    assert _grpc_status_code(exc) == grpc.StatusCode.NOT_FOUND
    result = _classify_milvus_exception(exc, WriteError("platform fallback"))
    assert isinstance(result, UserError)


def test_classify_milvus_exception_server_side_int_code_falls_through():
    # A server-side business status carries an int Milvus ErrorCode in .code
    # (not a grpc.StatusCode), so it must not be reclassified as a client error.
    from pymilvus import MilvusException

    exc = MilvusException(code=1, message="unexpected server error")
    platform_fallback = WriteError("platform fallback")

    result = _classify_milvus_exception(exc, platform_fallback)

    assert result is platform_fallback


# ---------------------------------------------------------------------------
# Integration: drive the real uploader methods through a mocked Milvus client
# that raises a REAL grpc.RpcError, exactly as pymilvus 2.6.9 re-raises it.
# ---------------------------------------------------------------------------


def _uploader() -> MilvusUploader:
    return MilvusUploader(
        connection_config=MilvusConnectionConfig(uri="http://localhost:19530"),
        upload_config=MilvusUploaderConfig(collection_name="test_collection"),
    )


def _with_client(uploader: MilvusUploader, client: MagicMock) -> None:
    @contextmanager
    def _cm():
        yield client

    uploader.get_client = _cm


@pytest.mark.parametrize(
    "status_name,expected",
    [
        ("UNAUTHENTICATED", UserAuthError),
        ("PERMISSION_DENIED", UserError),
        ("INVALID_ARGUMENT", UserError),
    ],
)
def test_precheck_reclassifies_raw_grpc_error(status_name: str, expected):
    uploader = _uploader()
    client = MagicMock()
    client.has_collection.side_effect = _FakeRpcError(getattr(grpc.StatusCode, status_name))
    _with_client(uploader, client)

    with pytest.raises(expected) as excinfo:
        uploader.precheck()
    # UserAuthError is a UserError subclass; assert we did not fall through to a
    # platform DestinationConnectionError.
    assert not isinstance(excinfo.value, DestinationConnectionError)


def test_precheck_missing_collection_is_user_error():
    uploader = _uploader()
    client = MagicMock()
    client.has_collection.return_value = False
    _with_client(uploader, client)

    with pytest.raises(UserError) as excinfo:
        uploader.precheck()
    assert not isinstance(excinfo.value, DestinationConnectionError)


def test_precheck_other_grpc_code_stays_platform():
    uploader = _uploader()
    client = MagicMock()
    client.has_collection.side_effect = _FakeRpcError(grpc.StatusCode.INTERNAL)
    _with_client(uploader, client)

    with pytest.raises(DestinationConnectionError):
        uploader.precheck()


def test_precheck_non_grpc_exception_stays_platform():
    # The @DestinationConnectionError.wrap catch-all fallback, reproduced inline.
    uploader = _uploader()
    client = MagicMock()
    client.has_collection.side_effect = RuntimeError("socket exploded")
    _with_client(uploader, client)

    with pytest.raises(DestinationConnectionError):
        uploader.precheck()


@pytest.mark.parametrize(
    "status_name,expected",
    [
        ("UNAUTHENTICATED", UserAuthError),
        ("INVALID_ARGUMENT", UserError),
    ],
)
def test_insert_results_reclassifies_raw_grpc_error(status_name: str, expected):
    uploader = _uploader()
    client = MagicMock()
    # dynamic-fields enabled short-circuits _prepare_data_for_insert to avoid a
    # second describe_collection; the failure we want to test is on insert().
    client.describe_collection.return_value = {"enable_dynamic_field": True}
    client.insert.side_effect = _FakeRpcError(getattr(grpc.StatusCode, status_name))
    _with_client(uploader, client)

    with pytest.raises(expected):
        uploader.insert_results(data=[{"embeddings": [0.1], "text": "x"}])


def test_insert_results_other_grpc_code_stays_write_error():
    uploader = _uploader()
    client = MagicMock()
    client.describe_collection.return_value = {"enable_dynamic_field": True}
    client.insert.side_effect = _FakeRpcError(grpc.StatusCode.INTERNAL)
    _with_client(uploader, client)

    with pytest.raises(WriteError) as excinfo:
        uploader.insert_results(data=[{"embeddings": [0.1], "text": "x"}])
    assert not isinstance(excinfo.value, UserError)


def test_delete_by_record_id_reclassifies_raw_grpc_error(file_data: FileData):
    uploader = _uploader()
    client = MagicMock()
    client.delete.side_effect = _FakeRpcError(grpc.StatusCode.PERMISSION_DENIED)
    _with_client(uploader, client)

    with pytest.raises(UserError):
        uploader.delete_by_record_id(file_data=file_data)


def test_prepare_data_reclassifies_raw_grpc_on_describe():
    # has_dynamic_fields_enabled() swallows the first describe error (best
    # effort) and returns False, so the non-dynamic branch calls
    # describe_collection again -- which must surface as a classified error.
    uploader = _uploader()
    client = MagicMock()
    client.describe_collection.side_effect = _FakeRpcError(grpc.StatusCode.UNAUTHENTICATED)
    _with_client(uploader, client)

    with pytest.raises(UserAuthError):
        uploader._prepare_data_for_insert(data=[{"embeddings": [0.1], "text": "x"}])


def test_run_data_missing_collection_delete_grpc_not_found_is_user_error(file_data: FileData):
    # run_data() -> delete_by_record_id(): a NOT_FOUND on delete is wrapped in a
    # MilvusException by pymilvus; still a client error, never platform.
    uploader = _uploader()
    client = MagicMock()
    client.delete.side_effect = _milvus_exception(grpc.StatusCode.NOT_FOUND)
    _with_client(uploader, client)

    with pytest.raises(UserError) as excinfo:
        uploader.run_data(data=[{"embeddings": [0.1]}], file_data=file_data)
    assert isinstance(excinfo.value, UnstructuredIngestError)
