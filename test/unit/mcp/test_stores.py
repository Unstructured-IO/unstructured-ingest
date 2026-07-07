"""Backend conformance suite for rag-ingest-mcp vector stores.

Every backend must pass the same tests; passing this suite is what "supported
backend" means. Chroma and Qdrant run embedded (a tmp directory, no service).
pgvector needs a reachable Postgres with the vector extension available — set
``URAG_TEST_PG_DSN`` (e.g. a throwaway ``pgvector/pgvector`` container) or its
cases skip.
"""

from __future__ import annotations

import os
import uuid

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.mcp.stores import EmbeddingSpace, SpaceMismatch, VectorStore

# Orthogonal 4-dim vectors: cosine ranking is exact, no embedding model needed.
TEXTS_TO_VECTORS = {
    "cats": [1.0, 0.0, 0.0, 0.0],
    "dogs": [0.0, 1.0, 0.0, 0.0],
    "stocks": [0.0, 0.0, 1.0, 0.0],
}
SPACE = EmbeddingSpace(provider="test", model="conformance-model", dimension=4)
OTHER_SPACE = EmbeddingSpace(provider="test", model="other-model", dimension=5)


def make_elements() -> list[dict]:
    return [
        {
            "element_id": f"element-{index}",
            "text": text,
            "embeddings": vector,
            "metadata": {"filename": "doc.txt"},
        }
        for index, (text, vector) in enumerate(TEXTS_TO_VECTORS.items())
    ]


def make_file_data(identifier: str = "doc-1") -> FileData:
    return FileData(
        identifier=identifier,
        connector_type="transform_mcp",
        source_identifiers=SourceIdentifiers(filename=identifier, fullpath=identifier),
    )


@pytest.fixture(params=["chroma", "qdrant", "pgvector"])
def store(request, tmp_path) -> VectorStore:
    if request.param == "chroma":
        pytest.importorskip("chromadb", reason="chroma backend requires the chroma extra")
        from unstructured_ingest.mcp.stores.chroma import ChromaStore

        yield ChromaStore(str(tmp_path / "chroma"))
        return
    if request.param == "qdrant":
        pytest.importorskip("qdrant_client", reason="qdrant backend requires the qdrant extra")
        from unstructured_ingest.mcp.stores.qdrant import QdrantStore

        yield QdrantStore(path=str(tmp_path / "qdrant"))
        return
    dsn = os.environ.get("URAG_TEST_PG_DSN")
    if not dsn:
        pytest.skip("URAG_TEST_PG_DSN not set; skipping pgvector conformance")
    pytest.importorskip("psycopg", reason="pgvector backend requires psycopg")
    from unstructured_ingest.mcp.stores.pgvector import PgvectorStore

    pg_store = PgvectorStore(dsn)
    yield pg_store
    # The database outlives the test run, so drop everything this test created.
    import psycopg

    with psycopg.connect(dsn) as conn:
        for name in pg_store.collections():
            if name.startswith("conf_"):
                conn.execute(f'DROP TABLE IF EXISTS "{name}"')
                conn.execute("DELETE FROM rag_ingest_spaces WHERE collection = %s", (name,))


@pytest.fixture
def collection() -> str:
    # Unique per test so a shared backend (pgvector) never sees collisions.
    return f"conf_{uuid.uuid4().hex[:12]}"


def load(store: VectorStore, collection: str, identifier: str = "doc-1") -> int:
    store.ensure_space(collection, SPACE)
    return store.write(collection, make_elements(), make_file_data(identifier))


def test_load_then_search_ranks_by_cosine(store, collection):
    assert load(store, collection) == 3
    matches = store.search(collection, [0.9, 0.1, 0.0, 0.0], k=3)
    assert [m["text"] for m in matches] == ["cats", "dogs", "stocks"]
    scores = [m["score"] for m in matches]
    assert scores == sorted(scores, reverse=True)
    assert scores[0] > 0.9


def test_search_returns_metadata(store, collection):
    load(store, collection)
    top = store.search(collection, [1.0, 0.0, 0.0, 0.0], k=1)[0]
    assert top["metadata"], "flattened element metadata should be returned"


def test_space_is_pinned_and_guarded(store, collection):
    load(store, collection)
    space = store.space_of(collection)
    assert (space.model, space.dimension) == (SPACE.model, SPACE.dimension)
    with pytest.raises(SpaceMismatch):
        store.ensure_space(collection, OTHER_SPACE)


def test_reload_upserts_instead_of_duplicating(store, collection):
    load(store, collection)
    load(store, collection)
    matches = store.search(collection, [1.0, 0.0, 0.0, 0.0], k=10)
    assert len(matches) == 3


def test_unembedded_elements_are_dropped(store, collection):
    store.ensure_space(collection, SPACE)
    elements = make_elements() + [{"element_id": "bare", "text": "no vector"}]
    assert store.write(collection, elements, make_file_data()) == 3


def test_collections_lists_what_was_loaded(store, collection):
    load(store, collection)
    assert collection in store.collections()


def test_space_of_unknown_collection_raises(store):
    with pytest.raises(Exception):
        store.space_of("conf_never_loaded")
