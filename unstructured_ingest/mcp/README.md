# rag-ingest-mcp

A small **local STDIO MCP server** that turns [Unstructured Transform
MCP](https://docs.unstructured.io) output into a queryable **local** RAG corpus.

Transform MCP does the heavy lifting on the platform — parse → chunk → *(optionally)*
embed — and hands back Element JSON. This server, running as a subprocess of your
MCP host, fetches that JSON **out of band**, embeds it locally if it wasn't
already, upserts it into a local vector store, and serves similarity search. It
reuses `unstructured-ingest`'s own embedders and Chroma connector, so the write
path is identical to the ingest pipeline's.

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
                     ▼   local Chroma directory (your corpus)
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
pip install -e ".[chroma,openai]"
pip install -r unstructured_ingest/mcp/requirements.txt   # fastmcp
```

## Configure

Copy `.env.example` and set at least `OPENAI_API_KEY`. Key settings:

| Env var | Default | Meaning |
|---|---|---|
| `OPENAI_API_KEY` | — | required; embeds queries (and corpus in local mode) |
| `URAG_CHROMA_PATH` | `~/.unstructured-rag/chroma` | where the corpus persists |
| `URAG_EMBED_MODEL` | `text-embedding-3-small` | must match Transform's model in passthrough |
| `URAG_EMBED_PROVIDER` | `openai` | embed provider |

## Wire it into your MCP host

See `example_mcp_config.json`. Register **two** servers: this one (local, STDIO,
`python -m unstructured_ingest.mcp`) and the remote Transform MCP (HTTP/OAuth).
In Claude Desktop use `claude_desktop_config.json`; in Claude Code use `.mcp.json`.

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

## Notes & limits

- **Chroma is the default** because it auto-creates collections and takes rows as
  dicts. LanceDB / Qdrant-local connectors exist in `unstructured-ingest` but
  need a **pre-provisioned, typed table** (the LanceDB uploader drops columns not
  already in the schema), so they aren't wired as the zero-config default here.
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
