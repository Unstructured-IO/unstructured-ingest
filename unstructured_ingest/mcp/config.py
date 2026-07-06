"""Environment-driven configuration for the ingest RAG MCP server.

Every setting has a default that makes the common case — a local Chroma store
embedding with OpenAI ``text-embedding-3-large`` — work with only an
``OPENAI_API_KEY`` set. The embed defaults intentionally mirror the Transform
MCP's own default embedder: in passthrough mode the corpus vectors were produced
by Transform, so the query side must default to the same provider/model to stay
comparable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# The Transform MCP's default embedder. Kept in sync so that a passthrough load
# (Transform embedded) and a query embedded by this server land in one space.
DEFAULT_EMBED_PROVIDER = "openai"
DEFAULT_EMBED_MODEL = "text-embedding-3-large"

# A JSON render of a text corpus (image base64 stripped) is large but bounded;
# this cap is a backstop against a runaway download, not a tuning knob.
_DEFAULT_MAX_FETCH_BYTES = 512 * 1024 * 1024


@dataclass(frozen=True)
class Config:
    store_backend: str
    chroma_path: str
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
            embed_provider=os.environ.get("URAG_EMBED_PROVIDER", DEFAULT_EMBED_PROVIDER),
            embed_model=os.environ.get("URAG_EMBED_MODEL", DEFAULT_EMBED_MODEL),
            openai_api_key=os.environ.get("OPENAI_API_KEY"),
            openai_base_url=os.environ.get("OPENAI_BASE_URL"),
            embed_batch_size=int(os.environ.get("URAG_EMBED_BATCH_SIZE", "64")),
            max_fetch_bytes=int(
                os.environ.get("URAG_MAX_FETCH_BYTES", str(_DEFAULT_MAX_FETCH_BYTES))
            ),
        )
