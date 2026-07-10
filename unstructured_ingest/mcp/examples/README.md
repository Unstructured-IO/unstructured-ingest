# Local agentic RAG with rag-ingest-mcp — examples

These guides set up a complete **local agentic RAG loop**: your agent (Claude
Code, Claude Desktop, or any MCP host) parses documents with the hosted
**Transform MCP**, lands the chunks in a **local vector database** through
**rag-ingest-mcp**, and then answers questions by searching that corpus — all
orchestrated by the agent itself, tool call by tool call.

Pick a backend and follow its guide end to end:

| Guide | Backend runs as | Reach for it when |
|---|---|---|
| [chroma/](chroma/README.md) | embedded — a local directory, no service | you want zero infrastructure; the default |
| [qdrant/](qdrant/README.md) | embedded local mode **or** a docker server | you want a dedicated vector DB with a web dashboard, or already run Qdrant |
| [pgvector/](pgvector/README.md) | your Postgres (docker or existing) | you already run Postgres, or want SQL access to the same corpus |

Every backend behaves identically through the MCP tools — same three tools,
same guarantees. Switching is one env var (`URAG_STORE_BACKEND`); nothing about
the Transform side or the agent's workflow changes.

## The loop, once

```
┌─ MCP host (your agent) ────────────────────────────────────────────┐
│                                                                    │
│  ① Transform MCP (remote, OAuth)                                   │
│     request_file_upload_url → PUT bytes → transform_files          │
│     → check_transform_status → get_transform_results               │
│     ⇒ a signed download_url per file (Element JSON)                │
│                                                                    │
│  ② rag-ingest-mcp (local, STDIO — this package)                    │
│     load_transform_output(download_url, collection)                │
│        fetches out of band — the JSON never enters agent context   │
│        embeds locally iff Transform didn't                         │
│     search(query, collection) ⇒ nearest chunks + scores            │
└────────────────────────────────────────────────────────────────────┘
                          │
                          ▼  your local vector DB (the corpus)
```

The agent drives both servers with ordinary tool calls. A typical session:

> **You:** Load `~/Contracts/msa-2026.pdf` into a collection called `contracts`,
> then tell me what the termination clause says.
>
> **Agent:** *(Transform: upload → chunk job → download_url; rag-ingest-mcp:
> load_transform_output → search("termination clause", "contracts") → answers
> from the returned chunks, citing them.)*

## Where embedding happens — the one decision

Transform always parses and chunks. Embedding can happen in either of two
places, and `load_transform_output`'s `embed_policy="auto"` (the default)
detects which by sniffing for vectors:

| | **Local embed** (recommended start) | **Transform embeds** (passthrough) |
|---|---|---|
| Transform `stages` | `{"chunk": {...}}` only | `{"chunk": {...}, "embed": {...}}` |
| Who embeds the corpus | rag-ingest-mcp, with your `OPENAI_API_KEY` | the platform, with its cloud model |
| Model used | `URAG_EMBED_MODEL` (default `text-embedding-3-small`) | whatever the embed stage ran — **pass it as `embed_model` on load** |
| Who embeds each query | rag-ingest-mcp — always, in the collection's pinned space | same |

Either way, **every query needs a provider key locally** — `search` embeds the
query text with the same model the corpus used. That symmetry is enforced, not
suggested: a collection is pinned to one embedding space `(provider, model,
dimension)` on first load, later loads with a different model are refused, and
queries are always embedded into the pinned space. Mixing models silently
returns garbage rankings; the pin is what makes that impossible.

## Prerequisites (all guides)

From the `unstructured-ingest` repo root:

```bash
pip install -e ".[chroma,openai]"                         # base + default backend
pip install -r unstructured_ingest/mcp/requirements.txt   # fastmcp
export OPENAI_API_KEY=sk-...                              # query + local-corpus embedding
```

You also need access to a **Transform MCP** endpoint (the hosted Unstructured
Platform server, or an SND's). Your MCP host handles its OAuth on first use.

## Other MCP servers in the mix

- **Embeddings.** You don't need a separate embedding MCP: Transform's `embed`
  stage *is* the cloud-embedding service (passthrough mode), and rag-ingest-mcp
  embeds locally otherwise. Today the local path supports `openai`; more
  `unstructured_ingest.embed` providers are planned.
- **The DB vendor's own MCP.** Each guide has a "pairing with the vendor MCP"
  section. Short version: vendor MCPs are great for *admin and inspection*, but
  for chunk-level semantic search over a Transform-built corpus, query through
  rag-ingest-mcp — most vendor MCPs either can't embed the query at all
  (they expect a precomputed vector) or would embed it with a different model
  than the corpus (wrong space, garbage results). rag-ingest-mcp exists
  precisely to be the matched-space query front-end.

## Troubleshooting (all backends)

- **`SpaceMismatch: collection X was built with model-A … refusing model-B`** —
  working as designed. Load into a new collection, or pass the matching
  `embed_model`.
- **`could not embed query in the collection's space`** — `OPENAI_API_KEY` is
  missing/invalid, or the collection was passthrough-loaded under a model name
  your key can't run. Check `list_collections` for the pinned model.
- **Passthrough load pinned the wrong model** — in passthrough, the space is
  recorded from `embed_model` (or the server default). If Transform embedded
  with something else, dimensions won't match at query time. Re-load with the
  correct `embed_model`.
- **Elements loaded = 0** — the source had no vectors and `embed_policy` was
  `"passthrough"`. Use `"auto"` or `"local"`.
