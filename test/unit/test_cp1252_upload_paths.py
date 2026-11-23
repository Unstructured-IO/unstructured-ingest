"""Remaining upload paths must read staged JSON as UTF-8 when the platform default is cp1252.

``get_json_data`` / ``write_data`` already pass ``encoding="utf-8"``. The NDJSON
upload stagers and the LanceDB / Vectara readers open the staged file themselves.
On Windows that open decodes as cp1252, so a typographic quote (UTF-8 byte 0x9d)
raises ``UnicodeDecodeError`` in the upload stage.
"""

import builtins
import json
from pathlib import Path

import pandas as pd
import pyarrow  # noqa: F401  (loaded before the cp1252 patch; feather writes through it)
import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.interfaces.upload_stager import UploadStager, UploadStagerConfig
from unstructured_ingest.processes.connectors.lancedb.lancedb import LanceDBUploadStager
from unstructured_ingest.processes.connectors.pinecone import PineconeUploadStager
from unstructured_ingest.processes.connectors.vectara import VectaraUploadStager
from unstructured_ingest.processes.utils.blob_storage import (
    BlobStoreUploadStager,
    BlobStoreUploadStagerConfig,
)
from unstructured_ingest.utils import ndjson

# Same payload as the data_prep regression: the closing quote encodes to byte 0x9d,
# which cp1252 rejects.
NON_ASCII = "Привет, мир — 你好 “quoted”"


@pytest.fixture
def cp1252_default_encoding(monkeypatch):
    """Simulate Windows, where open() without an encoding decodes as cp1252.

    Both ``Path.open`` and the builtin are patched. LanceDB and Vectara's writer
    use the builtin, so a ``Path.open``-only patch would miss them.
    """
    original_path_open = Path.open
    original_builtin_open = builtins.open

    def path_open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        if "b" not in mode and encoding is None:
            encoding = "cp1252"
        if "b" in mode:
            return original_path_open(self, mode, buffering)
        return original_path_open(self, mode, buffering, encoding, errors, newline)

    def builtin_open(
        file,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        opener=None,
    ):
        if "b" not in str(mode) and encoding is None:
            encoding = "cp1252"
        if "b" in str(mode):
            return original_builtin_open(file, mode, buffering, closefd=closefd, opener=opener)
        return original_builtin_open(
            file, mode, buffering, encoding, errors, newline, closefd, opener
        )

    monkeypatch.setattr(Path, "open", path_open)
    monkeypatch.setattr(builtins, "open", builtin_open)


def _file_data() -> FileData:
    return FileData(
        identifier="rec-1",
        connector_type="test",
        source_identifiers=SourceIdentifiers(filename="doc.pdf", fullpath="doc.pdf"),
    )


def _write_utf8(path: Path, text: str) -> None:
    path.write_bytes(text.encode("utf-8"))


def _element() -> dict:
    return {"element_id": "a", "text": NON_ASCII, "embeddings": [0.1, 0.2]}


def test_upload_stager_ndjson_round_trips_under_cp1252(tmp_path: Path, cp1252_default_encoding):
    src = tmp_path / "in.ndjson"
    _write_utf8(src, json.dumps(_element(), ensure_ascii=False) + "\n")

    out = UploadStager(upload_stager_config=UploadStagerConfig()).run(
        elements_filepath=src,
        file_data=_file_data(),
        output_dir=tmp_path / "out",
        output_filename="in.ndjson",
    )

    assert ndjson.load(out.open(encoding="utf-8")) == [_element()]


def test_blob_store_ndjson_round_trips_under_cp1252(tmp_path: Path, cp1252_default_encoding):
    src = tmp_path / "in.ndjson"
    _write_utf8(src, json.dumps(_element(), ensure_ascii=False) + "\n")
    stager = BlobStoreUploadStager(upload_stager_config=BlobStoreUploadStagerConfig())

    out = stager.run(
        elements_filepath=src,
        file_data=_file_data(),
        output_dir=tmp_path / "out",
        output_filename="in.ndjson",
    )

    assert json.loads(out.read_bytes().decode("utf-8")) == [_element()]


def test_pinecone_ndjson_round_trips_under_cp1252(tmp_path: Path, cp1252_default_encoding):
    src = tmp_path / "in.ndjson"
    _write_utf8(src, json.dumps(_element(), ensure_ascii=False) + "\n")

    out = PineconeUploadStager().run(
        elements_filepath=src,
        file_data=_file_data(),
        output_dir=tmp_path / "out",
        output_filename="in.ndjson",
    )

    rows = ndjson.load(out.open(encoding="utf-8"))
    assert rows[0]["values"] == [0.1, 0.2]
    assert rows[0]["metadata"]["text"] == NON_ASCII


def test_lancedb_json_round_trips_under_cp1252(tmp_path: Path, cp1252_default_encoding):
    src = tmp_path / "in.json"
    _write_utf8(src, json.dumps([_element()], ensure_ascii=False))

    # LanceDBUploadStager.run writes the feather file itself and does not create
    # output_dir (the base stager's get_output_path does). The pipeline passes a
    # directory that already exists.
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    out = LanceDBUploadStager().run(
        elements_filepath=src,
        file_data=_file_data(),
        output_dir=out_dir,
        output_filename="in.json",
    )

    frame = pd.read_feather(out)
    assert frame["text"].tolist() == [NON_ASCII]
    assert frame["record_id"].tolist() == ["rec-1"]


def test_vectara_json_round_trips_under_cp1252(tmp_path: Path, cp1252_default_encoding):
    src = tmp_path / "in.json"
    _write_utf8(src, json.dumps([_element()], ensure_ascii=False))

    out = VectaraUploadStager().run(
        elements_filepath=src,
        file_data=_file_data(),
        output_dir=tmp_path / "out",
        output_filename="in.json",
    )

    document = json.loads(out.read_bytes().decode("utf-8"))
    assert document[0]["document_parts"][0]["text"] == NON_ASCII
