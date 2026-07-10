"""The backend contract every rag-ingest-mcp vector store implements.

A backend is "supported" when it upholds five invariants behind this interface:

1. **Space pin** — a collection records the embedding space (provider, model,
   dimension) its vectors were produced by; a load with a different space is
   refused (:class:`SpaceMismatch`). Vectors from different models are not
   comparable, so mixing them silently ruins retrieval.
2. **Auto-provision** — the first load creates the collection/table from the
   pinned space; no pre-provisioning step is asked of the user.
3. **Idempotent upsert** — element ids are deterministic, so re-loading a
   source overwrites rows rather than duplicating them.
4. **Cosine scoring** — ``search`` returns ``score`` as cosine similarity
   (higher is better), whatever the backend's native distance convention.
5. **Connector-shaped writes** — rows are conformed by the corresponding
   ``unstructured-ingest`` stager, so what lands in the store matches what the
   ingest pipeline's own connector would produce.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from unstructured_ingest.data_types.file_data import FileData


class SpaceMismatch(Exception):
    """Raised when a load would mix a second embedding space into a collection."""


@dataclass
class EmbeddingSpace:
    """The (provider, model, dimension) a collection's vectors were produced by."""

    provider: str
    model: str
    dimension: int

    def as_metadata(self) -> dict[str, Any]:
        return {
            "embed_provider": self.provider,
            "embed_model": self.model,
            "embed_dim": self.dimension,
        }

    @classmethod
    def from_metadata(cls, meta: dict[str, Any] | None) -> "EmbeddingSpace | None":
        if not meta or "embed_model" not in meta:
            return None
        return cls(
            provider=meta.get("embed_provider", ""),
            model=meta["embed_model"],
            dimension=int(meta["embed_dim"]),
        )

    def mismatch_error(self, collection: str, incoming: "EmbeddingSpace") -> SpaceMismatch:
        return SpaceMismatch(
            f"collection {collection!r} was built with {self.model} "
            f"({self.dimension}-dim); refusing to load {incoming.model} "
            f"({incoming.dimension}-dim) into it. Use a different collection name."
        )

    def conflicts_with(self, other: "EmbeddingSpace") -> bool:
        return (self.model, self.dimension) != (other.model, other.dimension)


class VectorStore(ABC):
    """Write, guard, and search one backend's collections."""

    @abstractmethod
    def ensure_space(self, collection: str, space: EmbeddingSpace) -> None:
        """Create ``collection`` pinned to ``space``, or verify an existing one.

        Raises :class:`SpaceMismatch` if the collection already holds vectors
        from a different model/dimension.
        """

    @abstractmethod
    def write(self, collection: str, elements: list[dict], file_data: FileData) -> int:
        """Upsert ``elements`` into ``collection``; return the rows written.

        Rows without a vector (an element that arrived un-embedded) are dropped —
        they can be neither stored nor searched. ``ensure_space`` must have run
        first so the collection exists with the intended dimension and metric.
        """

    @abstractmethod
    def space_of(self, collection: str) -> EmbeddingSpace:
        """The space ``collection`` is pinned to; raises if it has none."""

    @abstractmethod
    def search(self, collection: str, query_vector: list[float], k: int) -> list[dict[str, Any]]:
        """The ``k`` nearest rows as ``{score, text, metadata}`` dicts."""

    @abstractmethod
    def collections(self) -> list[str]:
        """Names of the collections this store holds."""
