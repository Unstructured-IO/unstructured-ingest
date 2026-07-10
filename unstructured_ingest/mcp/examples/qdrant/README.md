# Getting started: agentic RAG with Qdrant

Qdrant gives you a dedicated vector database with a web dashboard for browsing
your corpus. rag-ingest-mcp supports it in two modes — pick one:

- **Embedded local mode** — Qdrant runs *inside* the rag-ingest-mcp process
  against a local directory. Zero infrastructure, like Chroma. Good for
  getting started.
- **Server mode** — a Qdrant instance (docker below, or one you already run).
  Survives independent of the MCP process, and you get the dashboard.

## 1. Install

From the `unstructured-ingest` repo root:

```bash
pip install -e ".[qdrant,openai]"
pip install -r unstructured_ingest/mcp/requirements.txt   # fastmcp
```

## 2a. Embedded mode (no service)

Nothing to start. In the MCP config below, set only:

```json
"URAG_STORE_BACKEND": "qdrant",
"URAG_QDRANT_PATH": "~/.unstructured-rag/qdrant"
```

Note: embedded mode locks its directory to one process and has no dashboard.
When you outgrow it, switch to server mode — collections don't migrate
automatically, but re-running your load pipeline rebuilds them.

## 2b. Server mode (docker)

```bash
docker compose up -d          # uses this directory's docker-compose.yml
```

which runs:

```yaml
services:
  qdrant:
    image: qdrant/qdrant:latest
    ports: ["6333:6333"]
    volumes: ["qdrant-data:/qdrant/storage"]
```

Then in the MCP config:

```json
"URAG_STORE_BACKEND": "qdrant",
"URAG_QDRANT_URL": "http://localhost:6333"
```

(`URAG_QDRANT_URL` takes precedence over the path; add `URAG_QDRANT_API_KEY`
for a secured instance.)

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
        "URAG_STORE_BACKEND": "qdrant",
        "URAG_QDRANT_URL": "http://localhost:6333"
      }
    },
    "unstructured-transform": {
      "type": "http",
      "url": "https://<your-transform-mcp-host>/transform-mcp/mcp"
    }
  }
}
```

## 4. Run the loop

Ask your agent:

> Parse everything in `~/Research/papers/` with Transform (chunk by title),
> load it into a collection called `papers`, then: what methods do these papers
> use for evaluation?

The agent will drive Transform (upload → chunk job → `download_url` per file),
hand every URL to `load_transform_output(sources=[...], collection="papers")` —
directories of Element JSON also work as a source — and then call
`search("evaluation methods", collection="papers")` and synthesize an answer
from the returned chunks.

Re-loading the same sources upserts (deterministic ids); a load with a
different embedding model than the collection was built with is refused.

## 5. Inspect what landed

Server mode has the nicest inspection story of the three backends — a built-in
web UI:

```bash
open http://localhost:6333/dashboard
```

Or the REST API:

```bash
# collections (you'll also see __rag_ingest_meta__ — see below)
curl -s http://localhost:6333/collections | python -m json.tool

# config + point count: 1536-dim cosine for the default embed model
curl -s http://localhost:6333/collections/papers | python -m json.tool

# browse points with payloads (text, flattened element metadata)
curl -s -X POST http://localhost:6333/collections/papers/points/scroll \
  -H 'content-type: application/json' \
  -d '{"limit": 3, "with_payload": true}' | python -m json.tool
```

**About `__rag_ingest_meta__`:** Qdrant collections carry no free-form
metadata, so rag-ingest-mcp pins each collection's embedding space as one point
in this reserved meta collection. Inspect the pins with a scroll on it; don't
write to it or delete it (deleting orphans the space guard — collections would
re-pin on their next load).

Also normal: `indexed_vectors_count: 0` on small collections. Qdrant serves
search from raw segments until a collection is large enough (~10k+ vectors) to
be worth building the HNSW graph.

## Pairing with Qdrant's own MCP server

The official [`mcp-server-qdrant`](https://github.com/qdrant/mcp-server-qdrant)
embeds queries with **FastEmbed** (local ONNX models). It *cannot* reproduce
the OpenAI vectors your corpus was embedded with, so pointing it at a
rag-ingest-mcp collection gives confidently wrong results — the query lands in
a different embedding space than the corpus. Use it only for corpora it built
itself. For this corpus, `search` on rag-ingest-mcp is the correct query path
(it re-embeds each query in the collection's pinned space); the dashboard and
REST API cover inspection.

## Qdrant-specific notes

- Distance is cosine, configured at collection creation from the pinned space;
  Qdrant natively rejects wrong-dimension upserts as a second line of defense.
- Scores from `search` are cosine similarity as Qdrant reports it — directly
  comparable with the other backends' scores.
- No practical dimension ceiling for common embedding models (65k max).
