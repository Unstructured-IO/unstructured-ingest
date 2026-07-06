"""pgvector-backed vector store: a collection per table in your Postgres.

Rows are conformed by ``unstructured-ingest``'s SQL stager (Element JSON -> one
flat row per element, metadata lifted to top-level keys), then landed in a
schema this store owns: ``id`` (deterministic, the upsert key), ``record_id``
(the per-source identity), ``text``, ``metadata`` (the stager's row as JSONB),
and ``embedding vector(dim)``. The ingest SQL connector fits rows to whatever
table you provide; here the pinned space is known at first load, so the table —
and its cosine HNSW index — are created from it instead.

The space pin lives in one small table (``rag_ingest_spaces``), which is also
the registry of collections this store owns. pgvector caps HNSW-indexable
vectors at 2,000 dims; wider spaces still work but search is a sequential scan
(or re-create the index over ``halfvec`` yourself).

Uses psycopg 3; connect with a DSN like ``postgresql://user:pass@host:5432/db``.
The ``vector`` extension is created if missing (needs a privileged role once).
"""

from __future__ import annotations

import json
import re
from typing import Any

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.mcp.stores.base import EmbeddingSpace, VectorStore
from unstructured_ingest.processes.connectors.sql.sql import SQLUploadStager
from unstructured_ingest.utils.constants import RECORD_ID_LABEL

_SPACES_TABLE = "rag_ingest_spaces"

# pgvector refuses HNSW/ivfflat indexes above this dimension; wider vectors are
# stored and searched brute-force instead.
_MAX_INDEXABLE_DIM = 2000

# Postgres identifier: quoted names could carry anything, but keeping collection
# names plain identifiers keeps them usable from psql and other tools too.
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")


def _vector_literal(vector: list[float]) -> str:
    return json.dumps(vector)


def _check_name(collection: str) -> str:
    if not _IDENTIFIER.match(collection):
        raise ValueError(
            f"collection {collection!r} is not usable as a Postgres table name; "
            "use letters, digits and underscores, starting with a letter (max 63 chars)"
        )
    return collection


class PgvectorStore(VectorStore):
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def _connect(self):
        import psycopg

        return psycopg.connect(self._dsn)

    def _ensure_spaces_table(self, conn) -> None:
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_SPACES_TABLE} (
                collection TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                dimension INTEGER NOT NULL
            )
            """
        )

    def _read_pin(self, conn, collection: str) -> EmbeddingSpace | None:
        row = conn.execute(
            f"SELECT provider, model, dimension FROM {_SPACES_TABLE} WHERE collection = %s",
            (collection,),
        ).fetchone()
        if row is None:
            return None
        return EmbeddingSpace(provider=row[0], model=row[1], dimension=row[2])

    def ensure_space(self, collection: str, space: EmbeddingSpace) -> None:
        from psycopg import sql

        _check_name(collection)
        with self._connect() as conn:
            self._ensure_spaces_table(conn)
            existing = self._read_pin(conn, collection)
            if existing is not None:
                if existing.conflicts_with(space):
                    raise existing.mismatch_error(collection, space)
                return
            conn.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {table} (
                        id TEXT PRIMARY KEY,
                        record_id TEXT,
                        text TEXT,
                        metadata JSONB,
                        embedding vector({dim})
                    )
                    """
                ).format(table=sql.Identifier(collection), dim=sql.Literal(space.dimension))
            )
            if space.dimension <= _MAX_INDEXABLE_DIM:
                conn.execute(
                    sql.SQL(
                        "CREATE INDEX IF NOT EXISTS {index} ON {table} "
                        "USING hnsw (embedding vector_cosine_ops)"
                    ).format(
                        index=sql.Identifier(f"{collection}_embedding_hnsw"),
                        table=sql.Identifier(collection),
                    )
                )
            conn.execute(
                f"INSERT INTO {_SPACES_TABLE} (collection, provider, model, dimension) "
                "VALUES (%s, %s, %s, %s)",
                (collection, space.provider, space.model, space.dimension),
            )

    def write(self, collection: str, elements: list[dict], file_data: FileData) -> int:
        from psycopg import sql
        from psycopg.types.json import Json

        _check_name(collection)
        stager = SQLUploadStager()
        rows = [
            stager.conform_dict(element_dict=element, file_data=file_data) for element in elements
        ]
        params = []
        for row in rows:
            vector = row.pop("embeddings", None)
            if not vector:
                continue
            params.append(
                (
                    row.pop("id"),
                    row.pop(RECORD_ID_LABEL, file_data.identifier),
                    row.pop("text", None),
                    # The stager's remaining flat row is the metadata; default=str
                    # absorbs dates and other non-JSON scalars it leaves in place.
                    Json(row, dumps=lambda obj: json.dumps(obj, default=str)),
                    _vector_literal(vector),
                )
            )
        if not params:
            return 0
        statement = sql.SQL(
            """
            INSERT INTO {table} (id, record_id, text, metadata, embedding)
            VALUES (%s, %s, %s, %s, %s::vector)
            ON CONFLICT (id) DO UPDATE SET
                record_id = EXCLUDED.record_id,
                text = EXCLUDED.text,
                metadata = EXCLUDED.metadata,
                embedding = EXCLUDED.embedding
            """
        ).format(table=sql.Identifier(collection))
        with self._connect() as conn, conn.cursor() as cursor:
            cursor.executemany(statement, params)
        return len(params)

    def space_of(self, collection: str) -> EmbeddingSpace:
        _check_name(collection)
        with self._connect() as conn:
            self._ensure_spaces_table(conn)
            space = self._read_pin(conn, collection)
        if space is None:
            raise ValueError(f"collection {collection!r} has no recorded embedding space")
        return space

    def search(self, collection: str, query_vector: list[float], k: int) -> list[dict[str, Any]]:
        from psycopg import sql

        _check_name(collection)
        literal = _vector_literal(query_vector)
        statement = sql.SQL(
            """
            SELECT text, metadata, 1 - (embedding <=> %s::vector) AS score
            FROM {table}
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """
        ).format(table=sql.Identifier(collection))
        with self._connect() as conn:
            rows = conn.execute(statement, (literal, literal, max(1, k))).fetchall()
        return [
            {"score": score, "text": text, "metadata": metadata} for text, metadata, score in rows
        ]

    def collections(self) -> list[str]:
        with self._connect() as conn:
            self._ensure_spaces_table(conn)
            rows = conn.execute(
                f"SELECT collection FROM {_SPACES_TABLE} ORDER BY collection"
            ).fetchall()
        return [row[0] for row in rows]
