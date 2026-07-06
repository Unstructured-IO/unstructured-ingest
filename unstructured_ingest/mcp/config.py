"""Environment-driven configuration for the rag-ingest-mcp server.

Every setting has a default that makes the common case — a local Chroma store
embedding with OpenAI ``text-embedding-3-small`` — work with only an
``OPENAI_API_KEY`` set. In passthrough mode the corpus vectors were produced by
Transform, so the space recorded (and used for every query) must name the model
Transform actually embedded with — pass ``embed_model`` on load when it differs
from this default.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# text-embedding-3-small (1536 dims): cheap, strong default for a local corpus,
# and its dimension fits every candidate backend's ANN-index limits (pgvector
# caps HNSW-indexable vectors at 2000 dims, which 3-large's 3072 exceeds).
DEFAULT_EMBED_PROVIDER = "openai"
DEFAULT_EMBED_MODEL = "text-embedding-3-small"

# A JSON render of a text corpus (image base64 stripped) is large but bounded;
# this cap is a backstop against a runaway download, not a tuning knob.
_DEFAULT_MAX_FETCH_BYTES = 512 * 1024 * 1024


@dataclass(frozen=True)
class Config:
    store_backend: str
    chroma_path: str
    qdrant_path: str
    qdrant_url: str | None
    qdrant_api_key: str | None
    pg_dsn: str | None
    embed_provider: str
    embed_model: str
    openai_api_key: str | None
    openai_base_url: str | None
    embed_batch_size: int
    max_fetch_bytes: int

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            store_backend=os.environ.get("URAG_STORE_BACKEND", "chroma"),
            chroma_path=os.path.expanduser(
                os.environ.get("URAG_CHROMA_PATH", "~/.unstructured-rag/chroma")
            ),
            # Qdrant runs embedded on a local path unless a server url is given.
            qdrant_path=os.path.expanduser(
                os.environ.get("URAG_QDRANT_PATH", "~/.unstructured-rag/qdrant")
            ),
            qdrant_url=os.environ.get("URAG_QDRANT_URL"),
            qdrant_api_key=os.environ.get("URAG_QDRANT_API_KEY"),
            pg_dsn=os.environ.get("URAG_PG_DSN"),
            embed_provider=os.environ.get("URAG_EMBED_PROVIDER", DEFAULT_EMBED_PROVIDER),
            embed_model=os.environ.get("URAG_EMBED_MODEL", DEFAULT_EMBED_MODEL),
            openai_api_key=os.environ.get("OPENAI_API_KEY"),
            openai_base_url=os.environ.get("OPENAI_BASE_URL"),
            embed_batch_size=int(os.environ.get("URAG_EMBED_BATCH_SIZE", "64")),
            max_fetch_bytes=int(
                os.environ.get("URAG_MAX_FETCH_BYTES", str(_DEFAULT_MAX_FETCH_BYTES))
            ),
        )
