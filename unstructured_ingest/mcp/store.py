"""Chroma-backed vector store for the ingest RAG MCP server.

Writes go through ``unstructured-ingest``'s Chroma stager (Element JSON -> Chroma
row) and its columnar transform, so what lands on disk matches what the ingest
pipeline's own Chroma connector would produce. Reads use the Chroma client
directly, because ingest connectors are write-only destinations — retrieval is
this server's own concern.

A collection's embedding space (provider, model, dimension) is pinned in its
metadata on first write and verified on every subsequent write. Vectors from
different models are not comparable, so mixing two models into one collection
silently ruins retrieval; refusing the second model is the guard that prevents
it. Chroma is the default because it auto-creates collections and accepts rows
as plain dicts; LanceDB/Qdrant-local need a pre-provisioned, typed table.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.processes.connectors.chroma import (
    ChromaUploader,
    ChromaUploadStager,
)

# Cosine matches the near-unit-length vectors these embedding models emit; set
# once at collection creation and never changed for the life of the collection.
_DISTANCE = "cosine"


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


class ChromaStore:
    def __init__(self, path: str) -> None:
        self._path = path
        self._client = None

    def _get_client(self):
        # One PersistentClient per process/path: Chroma keeps a single
        # SQLite-backed handle, so the space guard, the upsert, and search all
        # share this one rather than opening competing handles to the same dir.
        if self._client is None:
            import chromadb

            self._client = chromadb.PersistentClient(path=self._path)
        return self._client

    def _get_collection_or_none(self, name: str):
        try:
            return self._get_client().get_collection(name)
        except Exception:
            # Chroma raises a version-specific "not found" error; any failure to
            # open is treated as absent, and a genuine problem re-surfaces at the
            # create/upsert that follows.
            return None

    def ensure_space(self, collection: str, space: EmbeddingSpace) -> None:
        """Create ``collection`` pinned to ``space``, or verify an existing one.

        Raises :class:`SpaceMismatch` if the collection already holds vectors
        from a different model/dimension.
        """
        coll = self._get_collection_or_none(collection)
        if coll is None:
            self._get_client().create_collection(
                collection,
                metadata={"hnsw:space": _DISTANCE, **space.as_metadata()},
            )
            return
        existing = EmbeddingSpace.from_metadata(coll.metadata)
        if existing is not None and (existing.model, existing.dimension) != (
            space.model,
            space.dimension,
        ):
            raise SpaceMismatch(
                f"collection {collection!r} was built with {existing.model} "
                f"({existing.dimension}-dim); refusing to load {space.model} "
                f"({space.dimension}-dim) into it. Use a different collection name."
            )

    def write(self, collection: str, elements: list[dict], file_data: FileData) -> int:
        """Upsert ``elements`` into ``collection``; return the rows written.

        Rows without a vector (an element that arrived un-embedded) are dropped —
        they can be neither stored nor searched. ``ensure_space`` must have run
        first so the collection exists with the intended distance metric.
        """
        stager = ChromaUploadStager()
        rows = [
            stager.conform_dict(element_dict=element, file_data=file_data)
            for element in elements
        ]
        rows = [row for row in rows if row.get("embedding")]
        if not rows:
            return 0
        # prepare_chroma_list is ingest's own row -> parallel-lists transform,
        # reused so this upsert is identical to the connector's.
        payload = ChromaUploader.prepare_chroma_list(tuple(rows))
        coll = self._get_client().get_collection(collection)
        coll.upsert(
            ids=payload["ids"],
            documents=payload["documents"],
            embeddings=payload["embeddings"],
            metadatas=payload["metadatas"],
        )
        return len(rows)

    def space_of(self, collection: str) -> EmbeddingSpace:
        coll = self._get_client().get_collection(collection)
        space = EmbeddingSpace.from_metadata(coll.metadata)
        if space is None:
            raise ValueError(
                f"collection {collection!r} has no recorded embedding space"
            )
        return space

    def search(
        self, collection: str, query_vector: list[float], k: int
    ) -> list[dict[str, Any]]:
        coll = self._get_client().get_collection(collection)
        res = coll.query(
            query_embeddings=[query_vector],
            n_results=max(1, k),
            include=["documents", "metadatas", "distances"],
        )
        matches: list[dict[str, Any]] = []
        for doc, meta, dist in zip(
            res["documents"][0], res["metadatas"][0], res["distances"][0]
        ):
            # Collection is cosine, so similarity = 1 - distance.
            matches.append({"score": 1.0 - dist, "text": doc, "metadata": meta})
        return matches

    def collections(self) -> list[str]:
        items = self._get_client().list_collections()
        # Chroma versions return either Collection objects or bare names here.
        return [item if isinstance(item, str) else item.name for item in items]
