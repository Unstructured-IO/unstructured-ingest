"""pgvector-backed vector store: a collection per table in your Postgres.

Rows are conformed by ``unstructured-ingest``'s SQL stager (Element JSON -> one
flat row per element, metadata lifted to top-level keys), then **fit to the
target table's columns** and landed in the Unstructured-compliant schema the SQL
pgvector connector uses: ``id`` (a deterministic uuid5, the upsert key), plus the
element columns the table actually has — ``type``, ``record_id``,
``element_id``, ``text``, the ``embeddings`` vector, and any wider metadata
columns (``filename``, ``page_number``, …) present in the table.

The key property this buys: the table this store writes is the *same* shape a
downstream reader that speaks the Unstructured schema expects (e.g. Dragon Lab's
pgvector index). So the collection can be pre-created by that reader and this
store loads straight into it, or — standalone — this store auto-provisions an
equivalent table from the pinned space. Either way the write path stays
pgvector-aware: the vector is sent as a JSON array literal cast to the column's
own vector type (``vector`` or ``halfvec``), which the generic SQL uploader does
not do.

The space pin lives in one small table (``rag_ingest_spaces``), which is also
the registry of collections this store owns; the element table carries no model
metadata of its own. pgvector caps HNSW-indexable vectors at 2,000 dims; wider
spaces still work but search is a sequential scan.

Uses psycopg 3; connect with a DSN like ``postgresql://user:pass@host:5432/db``.
The ``vector`` extension is created if missing (needs a privileged role once).
"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.mcp.stores.base import EmbeddingSpace, VectorStore
from unstructured_ingest.processes.connectors.sql.sql import SQLUploadStager

_SPACES_TABLE = "rag_ingest_spaces"

# pgvector refuses HNSW/ivfflat indexes above this dimension; wider vectors are
# stored and searched brute-force instead.
_MAX_INDEXABLE_DIM = 2000

# The vector-typed column this store writes/searches, and the Postgres vector
# types it may be declared as. ``embeddings`` (plural) is the Unstructured schema
# column name — the same one Dragon Lab's pgvector index uses.
_VECTOR_COLUMN = "embeddings"
_VECTOR_UDTS = ("vector", "halfvec", "sparsevec")

# Columns that are never returned as search-result metadata (they are the id,
# the document text, and the vector itself).
_NON_METADATA_COLUMNS = ("id", "text", _VECTOR_COLUMN)

# The schema auto-provisioned standalone (no pre-created table). Column order is
# the DDL order; ``id`` is the deterministic uuid5 upsert key. Every element
# column is nullable so a sparse element (e.g. one with no page number) still
# loads; a downstream reader that requires a subset simply sees the columns it
# needs. The vector column is appended separately with the pinned dimension.
_AUTOPROVISION_COLUMNS = ("id", "type", "record_id", "element_id", "text", "filename", "page_number")

# Postgres identifier: quoted names could carry anything, but keeping collection
# names plain identifiers keeps them usable from psql and other tools too.
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")

# Parses a declared vector type like ``halfvec(1536)`` into its dimension.
_VECTOR_TYPE_DIM = re.compile(r"\((\d+)\)")


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

    def _table_exists(self, conn, collection: str) -> bool:
        return conn.execute("SELECT to_regclass(%s)", (collection,)).fetchone()[0] is not None

    def _column_types(self, conn, collection: str) -> dict[str, str]:
        """Map of column name -> udt_name (e.g. ``text``, ``uuid``, ``halfvec``)."""
        rows = conn.execute(
            """
            SELECT column_name, udt_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            (collection,),
        ).fetchall()
        return {name: udt for name, udt in rows}

    def _vector_column(self, column_types: dict[str, str]) -> tuple[str, str]:
        """The vector column and its udt; prefers ``embeddings``, else any vector-typed column."""
        if column_types.get(_VECTOR_COLUMN) in _VECTOR_UDTS:
            return _VECTOR_COLUMN, column_types[_VECTOR_COLUMN]
        for name, udt in column_types.items():
            if udt in _VECTOR_UDTS:
                return name, udt
        raise ValueError(
            f"table has no vector-typed column (one of {list(_VECTOR_UDTS)}); "
            "is this an Unstructured-compliant pgvector table?"
        )

    def _existing_vector_dim(self, conn, collection: str, vector_column: str) -> int | None:
        """Declared dimension of an existing vector column, or None if unpardimensioned."""
        row = conn.execute(
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = %s::regclass AND attname = %s",
            (collection, vector_column),
        ).fetchone()
        if not row or not row[0]:
            return None
        match = _VECTOR_TYPE_DIM.search(row[0])
        return int(match.group(1)) if match else None

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

            if self._table_exists(conn, collection):
                # Pre-created by a downstream reader (e.g. Dragon Lab). Adopt it:
                # verify the vector dimension agrees, then record the pin. Never
                # recreate — the reader owns the table's schema.
                column_types = self._column_types(conn, collection)
                vector_column, _ = self._vector_column(column_types)
                declared_dim = self._existing_vector_dim(conn, collection, vector_column)
                if declared_dim is not None and declared_dim != space.dimension:
                    raise space.mismatch_error(
                        collection,
                        EmbeddingSpace(space.provider, space.model, declared_dim),
                    )
            else:
                # Standalone: auto-provision an Unstructured-compliant table.
                columns = ", ".join(f"{col} TEXT" for col in _AUTOPROVISION_COLUMNS if col != "id")
                conn.execute(
                    sql.SQL(
                        "CREATE TABLE IF NOT EXISTS {table} "
                        "(id UUID PRIMARY KEY, " + columns + ", "
                        "{vector_column} vector({dim}))"
                    ).format(
                        table=sql.Identifier(collection),
                        vector_column=sql.Identifier(_VECTOR_COLUMN),
                        dim=sql.Literal(space.dimension),
                    )
                )
                if space.dimension <= _MAX_INDEXABLE_DIM:
                    conn.execute(
                        sql.SQL(
                            "CREATE INDEX IF NOT EXISTS {index} ON {table} "
                            "USING hnsw ({vector_column} vector_cosine_ops)"
                        ).format(
                            index=sql.Identifier(f"{collection}_embeddings_hnsw"),
                            table=sql.Identifier(collection),
                            vector_column=sql.Identifier(_VECTOR_COLUMN),
                        )
                    )

            conn.execute(
                f"INSERT INTO {_SPACES_TABLE} (collection, provider, model, dimension) "
                "VALUES (%s, %s, %s, %s)",
                (collection, space.provider, space.model, space.dimension),
            )

    def _coerce(self, value: Any, udt: str):
        """Adapt a staged value to the target column's type (excluding the vector)."""
        from psycopg.types.json import Json

        if value is None:
            return None
        if udt == "uuid":
            return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
        if udt == "jsonb" or udt == "json":
            return Json(value, dumps=lambda obj: json.dumps(obj, default=str))
        if udt.startswith("_"):  # a Postgres array type (e.g. _text); psycopg adapts a list
            return value if isinstance(value, list) else [value]
        # text / varchar / numeric / … — JSON-encode structured values, else stringify
        # non-strings so they land in a TEXT column without a type-mismatch error.
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        return value if isinstance(value, str) else str(value)

    def write(self, collection: str, elements: list[dict], file_data: FileData) -> int:
        from psycopg import sql

        _check_name(collection)
        stager = SQLUploadStager()
        rows = [
            stager.conform_dict(element_dict=element, file_data=file_data) for element in elements
        ]

        with self._connect() as conn:
            column_types = self._column_types(conn, collection)
            vector_column, vector_udt = self._vector_column(column_types)

            # Fit to the table: keep only staged keys that are real columns, and
            # union across rows so every INSERT has a uniform column list.
            present = {key for row in rows for key in row}
            target_columns = [
                col for col in column_types if col in present and col != vector_column
            ]
            if "id" not in target_columns and "id" in column_types:
                target_columns.insert(0, "id")

            params = []
            for row in rows:
                vector = row.get("embeddings")
                if not vector:
                    continue  # un-embedded elements can be neither stored nor searched
                values = [self._coerce(row.get(col), column_types[col]) for col in target_columns]
                values.append(_vector_literal(vector))
                params.append(tuple(values))
            if not params:
                return 0

            insert_columns = target_columns + [vector_column]
            placeholders = [sql.SQL("%s")] * len(target_columns) + [
                sql.SQL("%s::") + sql.SQL(vector_udt)
            ]
            update_columns = [col for col in insert_columns if col != "id"]
            statement = sql.SQL(
                "INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
                "ON CONFLICT (id) DO UPDATE SET {assignments}"
            ).format(
                table=sql.Identifier(collection),
                columns=sql.SQL(", ").join(sql.Identifier(col) for col in insert_columns),
                placeholders=sql.SQL(", ").join(placeholders),
                assignments=sql.SQL(", ").join(
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                    for col in update_columns
                ),
            )
            with conn.cursor() as cursor:
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
        with self._connect() as conn:
            column_types = self._column_types(conn, collection)
            vector_column, vector_udt = self._vector_column(column_types)
            metadata_columns = [
                col for col in column_types if col not in _NON_METADATA_COLUMNS and col != vector_column
            ]

            selected = [sql.Identifier("text")] + [sql.Identifier(c) for c in metadata_columns]
            statement = sql.SQL(
                "SELECT {selected}, 1 - ({vector} <=> {q}::{udt}) AS score "
                "FROM {table} ORDER BY {vector} <=> {q}::{udt} LIMIT {k}"
            ).format(
                selected=sql.SQL(", ").join(selected),
                vector=sql.Identifier(vector_column),
                q=sql.Literal(literal),
                udt=sql.SQL(vector_udt),
                table=sql.Identifier(collection),
                k=sql.Literal(max(1, k)),
            )
            rows = conn.execute(statement).fetchall()

        results = []
        for row in rows:
            text = row[0]
            score = row[-1]
            metadata = {
                col: value
                for col, value in zip(metadata_columns, row[1:-1])
                if value is not None
            }
            results.append({"score": score, "text": text, "metadata": metadata})
        return results

    def collections(self) -> list[str]:
        with self._connect() as conn:
            self._ensure_spaces_table(conn)
            rows = conn.execute(
                f"SELECT collection FROM {_SPACES_TABLE} ORDER BY collection"
            ).fetchall()
        return [row[0] for row in rows]
