"""Server-level smoke tests for rag-ingest-mcp, offline.

Drives the FastMCP server in-memory with passthrough (pre-embedded) elements,
so no embedding provider key or running service is needed. Runs against both
embedded backends; pgvector's server path is covered by the store conformance
suite plus an integration environment.
"""

from __future__ import annotations

import json

import anyio
import pytest

pytest.importorskip("fastmcp", reason="rag-ingest-mcp requires fastmcp")

from fastmcp import Client  # noqa: E402

from unstructured_ingest.mcp.server import mcp  # noqa: E402

VECTORS = {
    "cats": [1.0, 0.0, 0.0, 0.0],
    "dogs": [0.0, 1.0, 0.0, 0.0],
    "stocks": [0.0, 0.0, 1.0, 0.0],
}


@pytest.fixture(params=["chroma", "qdrant"])
def backend_env(request, monkeypatch, tmp_path) -> str:
    pytest.importorskip("chromadb" if request.param == "chroma" else "qdrant_client")
    monkeypatch.setenv("URAG_STORE_BACKEND", request.param)
    monkeypatch.setenv("URAG_CHROMA_PATH", str(tmp_path / "chroma"))
    monkeypatch.setenv("URAG_QDRANT_PATH", str(tmp_path / "qdrant"))
    monkeypatch.setenv("URAG_EMBED_MODEL", "smoke-model")
    return request.param


def write_elements_file(path, texts_to_vectors=VECTORS) -> str:
    elements = [
        {
            "element_id": f"element-{index}",
            "text": text,
            "embeddings": vector,
            "metadata": {"filename": path.name},
        }
        for index, (text, vector) in enumerate(texts_to_vectors.items())
    ]
    path.write_text(json.dumps(elements))
    return str(path)


def call(tool: str, arguments: dict) -> dict:
    async def run():
        async with Client(mcp) as client:
            return (await client.call_tool(tool, arguments)).data

    return anyio.run(run)


def test_load_search_and_list(backend_env, tmp_path):
    source = write_elements_file(tmp_path / "doc.json")
    result = call(
        "load_transform_output",
        {"sources": [source], "collection": "smoke", "embed_policy": "passthrough"},
    )
    assert result["files"][0]["loaded"] == 3, result
    assert result["space"]["embed_dim"] == 4

    listed = call("list_collections", {})
    entry = next(c for c in listed["collections"] if c["collection"] == "smoke")
    assert entry["model"] == "smoke-model"
    assert entry["dimension"] == 4


def test_directory_source_expands_to_all_json(backend_env, tmp_path):
    docs = tmp_path / "transform-output"
    docs.mkdir()
    write_elements_file(docs / "a.json")
    write_elements_file(docs / "b.json")
    result = call(
        "load_transform_output",
        {"sources": [str(docs)], "collection": "smokedir", "embed_policy": "passthrough"},
    )
    loaded = [f for f in result["files"] if "loaded" in f]
    assert len(loaded) == 2 and all(f["loaded"] == 3 for f in loaded), result


def test_space_guard_reports_per_source_error(backend_env, tmp_path):
    call(
        "load_transform_output",
        {
            "sources": [write_elements_file(tmp_path / "a.json")],
            "collection": "smokeguard",
            "embed_policy": "passthrough",
        },
    )
    five_dim = {text: vector + [0.0] for text, vector in VECTORS.items()}
    result = call(
        "load_transform_output",
        {
            "sources": [write_elements_file(tmp_path / "b.json", five_dim)],
            "collection": "smokeguard",
            "embed_policy": "passthrough",
            "embed_model": "other-model",
        },
    )
    assert "refusing" in result["files"][0]["error"], result
