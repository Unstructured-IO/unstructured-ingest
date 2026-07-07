# rag-ingest-mcp

A small **local STDIO MCP server** that turns [Unstructured Transform
MCP](https://docs.unstructured.io) output into a queryable **local** RAG corpus.

Transform MCP does the heavy lifting on the platform — parse → chunk → *(optionally)*
embed — and hands back Element JSON. This server, running as a subprocess of your
MCP host, fetches that JSON **out of band**, embeds it locally if it wasn't
already, upserts it into a local vector store — **Chroma** (default), **Qdrant**,
or **pgvector** — and serves similarity search. It reuses `unstructured-ingest`'s
own embedders and connector stagers, so the write path is identical to the
ingest pipeline's.

```
┌─ MCP Host (Claude Code / Desktop) ────────────────────────────────┐
│  the agent orchestrates both servers; large JSON never enters its │
│  context — it only passes a download_url string                   │
│                                                                   │
│   ①  Transform MCP  (remote, OAuth)                               │
│        parse → chunk → [embed]   ⇒   Element JSON + download_url   │
│                                                                   │
│   ②  this server    (local, STDIO)                               │
│        load_transform_output(download_url) → local store          │
│        search(query) → nearest chunks                             │
└───────────────────────────────────────────────────────────────────┘
                     │
                     ▼   local vector store (your corpus): chroma | qdrant | pgvector
```

## The one design idea: embedding is the only knob

Everything is fixed except **where the embedding happens**, and this server
absorbs that choice per load via `embed_policy`:

| | **Transform embeds** (passthrough) | **This server embeds** (local) |
|---|---|---|
| Transform `stages` | `{chunk, embed}` | `{chunk}` only |
| JSON returned | text **+ vectors** | text, **no vectors** |
| This server does | stage → upsert as-is | `embed_documents()` → stage → upsert |
| Query vector | this server rebuilds the same model (needs the provider key here) | the *same encoder object* — symmetric by construction |
| Embedding billed | on the platform | on your local OpenAI key |

`embed_policy="auto"` (default) picks per source by sniffing whether vectors are
present, so the agent chooses simply by toggling Transform's `embed` stage.

**The rule this enforces:** a collection is pinned to one embedding space
(provider, model, dimension) on first load; a later load with a different model
is **refused**, and every query is embedded into that same space. That's what
keeps retrieval correct — mixing models silently returns nonsense.

## Install

From the `unstructured-ingest` repo root (this module lives inside the package):

```bash
pip install -e ".[chroma,openai]"                         # default backend
pip install -r unstructured_ingest/mcp/requirements.txt   # fastmcp
```

For the other backends, add what they need:

```bash
pip install -e ".[qdrant]"          # qdrant backend (embedded local mode or server)
pip install "psycopg[binary]"       # pgvector backend (psycopg 3)
```

## Configure

Copy `.env.example` and set at least `OPENAI_API_KEY`. Key settings:

| Env var | Default | Meaning |
|---|---|---|
| `OPENAI_API_KEY` | — | required; embeds queries (and corpus in local mode) |
| `URAG_STORE_BACKEND` | `chroma` | `chroma`, `qdrant`, or `pgvector` |
| `URAG_CHROMA_PATH` | `~/.unstructured-rag/chroma` | chroma: where the corpus persists |
| `URAG_QDRANT_PATH` | `~/.unstructured-rag/qdrant` | qdrant: embedded local-mode directory |
| `URAG_QDRANT_URL` | — | qdrant: server url (overrides local mode) |
| `URAG_QDRANT_API_KEY` | — | qdrant: server api key, if any |
| `URAG_PG_DSN` | — | pgvector: `postgresql://user:pass@host:5432/db` |
| `URAG_EMBED_MODEL` | `text-embedding-3-small` | must match Transform's model in passthrough |
| `URAG_EMBED_PROVIDER` | `openai` | embed provider |

## Wire it into your MCP host

Register **two** servers: this one (local, STDIO, `python -m
unstructured_ingest.mcp`) and the remote Transform MCP (HTTP/OAuth). In Claude
Desktop use `claude_desktop_config.json`; in Claude Code use `.mcp.json`.

Each supported backend has a getting-started guide with a ready-to-copy host
config — start with the [examples overview](examples/README.md), or jump
straight to your backend:

| Backend | Getting-started guide | Host config |
|---|---|---|
| **Chroma** (default, zero-infra) | [examples/chroma/](examples/chroma/README.md) | [mcp-config.json](examples/chroma/mcp-config.json) |
| **Qdrant** (embedded or docker server) | [examples/qdrant/](examples/qdrant/README.md) | [mcp-config.json](examples/qdrant/mcp-config.json) |
| **pgvector** (your Postgres) | [examples/pgvector/](examples/pgvector/README.md) | [mcp-config.json](examples/pgvector/mcp-config.json) |

Every guide covers setup, docker-compose where a service is needed, the full
agentic loop with Transform MCP, CLI inspection of the database, and when (not)
to pair the DB vendor's own MCP server.

## The flow the agent follows

1. **Transform MCP** — `request_file_upload_url` → `curl -X PUT` the bytes →
   `transform_files(file_refs, stages={"chunk": {...}, "embed": {}})`
   *(drop `embed` to embed locally instead)* → poll `check_transform_status` →
   `get_transform_results(job_id, output_format="json", image_base64="none")`
   → yields a `download_url` per file.
2. **This server** — `load_transform_output(sources=[download_url, ...],
   collection="my_docs")`. It fetches, embeds if needed, and upserts. Returns
   `{loaded, space, files}`.
3. **Query** — `search("your question", collection="my_docs", k=5)` → nearest
   chunks with scores and metadata; the agent answers from them.

## Tools

- **`load_transform_output(sources, collection, embed_policy="auto", embed_model=None)`**
  — fetch Element JSON (URLs or paths), embed per policy, upsert. Per-source
  errors don't sink the batch.
- **`search(query, collection, k=5)`** — embed the query in the collection's
  pinned space and return the nearest chunks.
- **`list_collections()`** — collections and the model each is pinned to.

## Backends

Three backends, all holding the same contract (space pinned per collection on
first load, mismatched loads refused, deterministic-id upsert, cosine scores,
rows conformed by the corresponding `unstructured-ingest` stager). The
conformance suite in `test/unit/mcp/test_stores.py` runs identically against
every backend and is the definition of "supported".

| Backend | Runs as | Rows go through | Space pin lives in |
|---|---|---|---|
| `chroma` (default) | embedded, a local directory | `ChromaUploadStager` | collection metadata |
| `qdrant` | embedded local mode, or a server via `URAG_QDRANT_URL` | `QdrantUploadStager` | a reserved meta collection |
| `pgvector` | your Postgres (`URAG_PG_DSN`) | `SQLUploadStager` → `{id, record_id, text, metadata JSONB, embedding vector(dim)}` | `rag_ingest_spaces` table |

The first load auto-provisions the collection/table from the pinned space — no
schema setup is asked of you. For pgvector that includes a cosine **HNSW index**
when the dimension allows it (pgvector caps indexable vectors at 2,000 dims;
wider spaces work but search sequentially — the default
`text-embedding-3-small`/1536 stays comfortably inside the cap).

## Notes & limits
- **Passthrough still needs a local provider key.** Transform embedded the
  corpus, but *this* server embeds each query, so `OPENAI_API_KEY` (matching
  Transform's model) must be set even when you never embed a corpus locally.
- **Passthrough records the model you tell it.** The space is pinned from
  `embed_model` (or the server default, `text-embedding-3-small`). If Transform
  embedded with a different model — its own default is `text-embedding-3-large` —
  pass `embed_model` on the load, or queries will be embedded in the wrong space
  and rejected on dimension mismatch.
- **Re-loading a source upserts** rather than duplicating: element ids are
  derived deterministically from the source identity, so re-running a load
  overwrites the same rows.
- Distance is **cosine**; `search` scores are `1 − cosine_distance`.
