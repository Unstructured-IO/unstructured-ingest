# Getting started: agentic RAG with pgvector (Postgres)

pgvector puts your RAG corpus in Postgres: every collection is a plain table
with a `vector` column, so the same data your agent searches semantically is
also one `psql` away — and one *SQL MCP server* away, which makes this the most
composable backend for agentic workflows (see the pairing section below).

rag-ingest-mcp auto-provisions everything on first load: the `vector`
extension, the collection's table, a cosine HNSW index, and a small
`rag_ingest_spaces` registry that pins each collection's embedding model. You
bring a running Postgres; no schema work.

## 1. Install

From the `unstructured-ingest` repo root:

```bash
pip install -e ".[openai]"
pip install -r unstructured_ingest/mcp/requirements.txt   # fastmcp
pip install "psycopg[binary]"                             # Postgres driver (psycopg 3)
```

## 2. Start Postgres (or use one you have)

```bash
docker compose up -d          # uses this directory's docker-compose.yml
```

which runs the official pgvector image:

```yaml
services:
  postgres:
    image: pgvector/pgvector:pg17
    environment: { POSTGRES_PASSWORD: postgres }
    ports: ["5432:5432"]
    volumes: ["pg-data:/var/lib/postgresql/data"]
```

Using an existing Postgres instead? Any 13+ instance with the
[pgvector extension](https://github.com/pgvector/pgvector) available works.
The first load runs `CREATE EXTENSION IF NOT EXISTS vector`, which needs a
sufficiently privileged role once; after that an ordinary role that can create
tables is enough.

## 3. Register both MCP servers with your agent

Copy [`mcp-config.json`](mcp-config.json) — Claude Code reads `.mcp.json` in
your project root; Claude Desktop reads `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "rag-ingest-mcp": {
      "command": "python",
      "args": ["-m", "unstructured_ingest.mcp"],
      "env": {
        "OPENAI_API_KEY": "sk-...",
        "URAG_STORE_BACKEND": "pgvector",
        "URAG_PG_DSN": "postgresql://postgres:postgres@localhost:5432/postgres"
      }
    },
    "unstructured-transform": {
      "type": "http",
      "url": "https://<your-transform-mcp-host>/transform-mcp/mcp"
    }
  }
}
```

One pgvector-specific rule: **collection names must be valid Postgres
identifiers** (letters, digits, underscores; start with a letter) because the
collection *is* the table — `contracts_2026` works, `contracts-2026` is
refused.

## 4. Run the loop

Ask your agent:

> Parse `~/Contracts/msa-2026.pdf` and `~/Contracts/sow-2026.pdf` with
> Transform, chunk by title, load them into `contracts`, then: what are our
> termination and liability terms?

The agent uploads both files through Transform, runs the chunk job, hands the
resulting `download_url`s to `load_transform_output(collection="contracts")`
(no vectors in chunk-only output, so rag-ingest-mcp embeds locally), and
answers from `search("termination and liability terms", "contracts")`.

Re-loading a source upserts via `ON CONFLICT` on the deterministic element id —
re-running a pipeline updates rows in place instead of duplicating them.

## 5. Inspect what landed — it's just Postgres

```bash
docker compose exec postgres psql -U postgres     # or: psql "$URAG_PG_DSN"
```

```sql
\dt                                   -- one table per collection + rag_ingest_spaces
\d contracts                          -- id, record_id, text, metadata jsonb, embedding vector(1536)
                                      -- plus the cosine HNSW index
SELECT * FROM rag_ingest_spaces;      -- which embedding model each collection is pinned to

-- chunks per source document
SELECT metadata->>'filename' AS file, count(*) FROM contracts GROUP BY 1;

-- full-text-ish triage without touching vectors
SELECT left(text, 80) FROM contracts WHERE text ILIKE '%termination%';
```

The `metadata` column holds the element's flattened metadata (filename, page
number, element type, …) as JSONB, so all of Postgres's JSON operators apply.

## Pairing with a Postgres MCP server — the agentic-SQL combo

This is where pgvector earns its keep. Register a general-purpose Postgres MCP
(e.g. [`postgres-mcp`](https://github.com/crystaldba/postgres-mcp)) alongside,
pointed at the same database, and your agent gets **two complementary views of
one corpus**:

- **rag-ingest-mcp** answers *"what does the corpus say about X?"* — semantic
  search, with the query embedded in the collection's pinned space.
- **the Postgres MCP** answers *"what's in the corpus?"* — counts, filters,
  grouping, joins against your other tables: *"how many chunks per document?"*,
  *"which contracts mention indemnification on pages 1–3?"* (JSONB filters),
  *"join chunk counts against my `vendors` table"*.

The division of labor matters: a generic SQL MCP can't do the semantic side
correctly on its own — pgvector search needs a query *vector*, and the SQL
server has no way to produce one in your corpus's embedding space. That
cloud-embed front-end is exactly the gap rag-ingest-mcp fills. Let each do what
it's for: don't hand-write `ORDER BY embedding <=> ...` through the SQL MCP,
and don't ask rag-ingest-mcp for analytics.

## pgvector-specific notes

- **Dimension cap for indexing:** pgvector's HNSW index tops out at 2,000
  dimensions. The default model (`text-embedding-3-small`, 1536) fits. Wider
  spaces (e.g. `text-embedding-3-large`, 3072) still load and search correctly
  but scan sequentially — fine for small corpora; for large ones, prefer a
  ≤2,000-dim model or convert the column to `halfvec` yourself.
- Scores are cosine similarity (`1 - (embedding <=> query)`), comparable with
  the other backends.
- `rag_ingest_spaces` is the source of truth for `list_collections`; dropping a
  collection means dropping its table *and* its row there.
