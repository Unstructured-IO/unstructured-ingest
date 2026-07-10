"""Qdrant-backed vector store: embedded local mode or a running server.

Writes are conformed by ``unstructured-ingest``'s Qdrant stager (Element JSON ->
point with ``id``/``vector``/``payload``), so what lands in Qdrant matches what
the ingest pipeline's own connector would produce. With ``path`` set the store
runs Qdrant's embedded local mode (a directory, no service); with ``url`` set it
talks to a server.

Qdrant collections carry no free-form metadata, so the embedding-space pin lives
in a reserved meta collection: one point per data collection, keyed
deterministically by collection name, its payload holding the space. The data
collection's own vector params (size, cosine) are derived from the pinned space
at creation, so the dimension is also enforced natively on every upsert.
"""

from __future__ import annotations

from typing import Any
from uuid import NAMESPACE_DNS, uuid5

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.mcp.stores.base import EmbeddingSpace, VectorStore
from unstructured_ingest.processes.connectors.qdrant.local import (
    LocalQdrantUploadStager,
    LocalQdrantUploadStagerConfig,
)

# Holds one space-pin point per data collection; never searched, never listed.
_META_COLLECTION = "__rag_ingest_meta__"

# Payload keys the stager adds that are storage detail, not search-result metadata.
_PAYLOAD_INTERNAL_KEYS = ("text", "element_serialized")


def _meta_point_id(collection: str) -> str:
    return str(uuid5(NAMESPACE_DNS, f"rag-ingest-space:{collection}"))


class QdrantStore(VectorStore):
    def __init__(
        self,
        path: str | None = None,
        url: str | None = None,
        api_key: str | None = None,
    ) -> None:
        if not path and not url:
            raise ValueError("QdrantStore needs a local path or a server url")
        self._path = path
        self._url = url
        self._api_key = api_key
        self._client = None

    def _get_client(self):
        # One client per process: local mode locks its directory, so the space
        # guard, the upsert, and search must share a single handle.
        if self._client is None:
            from qdrant_client import QdrantClient

            if self._url:
                self._client = QdrantClient(url=self._url, api_key=self._api_key)
            else:
                self._client = QdrantClient(path=self._path)
        return self._client

    def _read_pin(self, collection: str) -> EmbeddingSpace | None:
        client = self._get_client()
        if not client.collection_exists(_META_COLLECTION):
            return None
        points = client.retrieve(_META_COLLECTION, ids=[_meta_point_id(collection)])
        if not points:
            return None
        return EmbeddingSpace.from_metadata(points[0].payload)

    def _write_pin(self, collection: str, space: EmbeddingSpace) -> None:
        from qdrant_client import models

        client = self._get_client()
        if not client.collection_exists(_META_COLLECTION):
            client.create_collection(
                _META_COLLECTION,
                vectors_config=models.VectorParams(size=1, distance=models.Distance.COSINE),
            )
        client.upsert(
            _META_COLLECTION,
            points=[
                models.PointStruct(
                    id=_meta_point_id(collection),
                    vector=[0.0],
                    payload={"collection": collection, **space.as_metadata()},
                )
            ],
            wait=True,
        )

    def ensure_space(self, collection: str, space: EmbeddingSpace) -> None:
        from qdrant_client import models

        existing = self._read_pin(collection)
        if existing is not None:
            if existing.conflicts_with(space):
                raise existing.mismatch_error(collection, space)
            return
        client = self._get_client()
        if not client.collection_exists(collection):
            client.create_collection(
                collection,
                vectors_config=models.VectorParams(
                    size=space.dimension, distance=models.Distance.COSINE
                ),
            )
        # A pre-existing collection without a pin (created outside this server)
        # adopts the incoming space, mirroring the Chroma backend's behavior.
        self._write_pin(collection, space)

    def write(self, collection: str, elements: list[dict], file_data: FileData) -> int:
        from qdrant_client import models

        stager = LocalQdrantUploadStager(upload_stager_config=LocalQdrantUploadStagerConfig())
        rows = [
            stager.conform_dict(element_dict=element, file_data=file_data) for element in elements
        ]
        rows = [row for row in rows if row.get("vector")]
        if not rows:
            return 0
        self._get_client().upsert(
            collection,
            points=[models.PointStruct(**row) for row in rows],
            wait=True,
        )
        return len(rows)

    def space_of(self, collection: str) -> EmbeddingSpace:
        space = self._read_pin(collection)
        if space is None:
            raise ValueError(f"collection {collection!r} has no recorded embedding space")
        return space

    def search(self, collection: str, query_vector: list[float], k: int) -> list[dict[str, Any]]:
        res = self._get_client().query_points(
            collection,
            query=query_vector,
            limit=max(1, k),
            with_payload=True,
        )
        matches: list[dict[str, Any]] = []
        for point in res.points:
            payload = point.payload or {}
            metadata = {
                key: value for key, value in payload.items() if key not in _PAYLOAD_INTERNAL_KEYS
            }
            # Qdrant returns cosine scores as similarity already (higher = closer).
            matches.append(
                {"score": point.score, "text": payload.get("text"), "metadata": metadata}
            )
        return matches

    def collections(self) -> list[str]:
        response = self._get_client().get_collections()
        return [c.name for c in response.collections if c.name != _META_COLLECTION]
