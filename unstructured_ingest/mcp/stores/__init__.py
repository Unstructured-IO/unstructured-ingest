"""rag-ingest-mcp storage backends.

One :class:`VectorStore` implementation per backend, selected by
``URAG_STORE_BACKEND``. Backend client libraries are imported lazily inside
each store, so an install only needs the extras for the backend it uses.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from unstructured_ingest.mcp.stores.base import (
    EmbeddingSpace,
    SpaceMismatch,
    VectorStore,
)

if TYPE_CHECKING:
    from unstructured_ingest.mcp.config import Config

BACKENDS = ("chroma", "qdrant", "pgvector")


def build_store(cfg: "Config") -> VectorStore:
    """The configured backend's store, or ValueError with the fix spelled out."""
    if cfg.store_backend == "chroma":
        from unstructured_ingest.mcp.stores.chroma import ChromaStore

        return ChromaStore(cfg.chroma_path)
    if cfg.store_backend == "qdrant":
        from unstructured_ingest.mcp.stores.qdrant import QdrantStore

        return QdrantStore(path=cfg.qdrant_path, url=cfg.qdrant_url, api_key=cfg.qdrant_api_key)
    if cfg.store_backend == "pgvector":
        if not cfg.pg_dsn:
            raise ValueError(
                "the pgvector backend needs URAG_PG_DSN set "
                "(e.g. postgresql://user:pass@localhost:5432/db)"
            )
        from unstructured_ingest.mcp.stores.pgvector import PgvectorStore

        return PgvectorStore(cfg.pg_dsn)
    raise ValueError(
        f"unknown store backend {cfg.store_backend!r}; "
        f"URAG_STORE_BACKEND must be one of {list(BACKENDS)}"
    )


__all__ = [
    "BACKENDS",
    "EmbeddingSpace",
    "SpaceMismatch",
    "VectorStore",
    "build_store",
]
