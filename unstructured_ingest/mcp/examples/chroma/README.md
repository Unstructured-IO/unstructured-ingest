# Getting started: agentic RAG with Chroma (the zero-infra default)

Chroma is rag-ingest-mcp's default backend: an embedded vector store that lives
in a local directory. **Nothing to install beyond the Python extras, nothing to
run, nothing to provision** — the first document you load creates the
collection. Start here unless you have a reason not to.

## 1. Install

From the `unstructured-ingest` repo root:

```bash
pip install -e ".[chroma,openai]"
pip install -r unstructured_ingest/mcp/requirements.txt   # fastmcp
```

## 2. Register both MCP servers with your agent

Copy [`mcp-config.json`](mcp-config.json) into your host's config — Claude Code
reads `.mcp.json` in the project root; Claude Desktop reads
`claude_desktop_config.json`. Fill in your OpenAI key and your Transform MCP
URL:

```json
{
  "mcpServers": {
    "rag-ingest-mcp": {
      "command": "python",
      "args": ["-m", "unstructured_ingest.mcp"],
      "env": {
        "OPENAI_API_KEY": "sk-...",
        "URAG_CHROMA_PATH": "~/.unstructured-rag/chroma"
      }
    },
    "unstructured-transform": {
      "type": "http",
      "url": "https://<your-transform-mcp-host>/transform-mcp/mcp"
    }
  }
}
```

That's the entire setup. `URAG_STORE_BACKEND` defaults to `chroma`;
`URAG_CHROMA_PATH` is where your corpus persists (this directory **is** your
RAG database — back it up, or delete it to start over).

## 3. Run the loop

Ask your agent, in plain language:

> Parse `~/Docs/quarterly-report.pdf` with Transform (chunk by title), load it
> into a collection called `reports`, then answer: what were the main revenue
> drivers?

Behind that one request the agent will:

1. `request_file_upload_url` → `PUT` the bytes → `transform_files` with
   `{"stages": {"chunk": {"strategy": "chunk_by_title"}}}`
2. poll `check_transform_status` until `COMPLETED`
3. `get_transform_results(job_id, output_format="json")` → a `download_url`
4. `load_transform_output(sources=[download_url], collection="reports")` —
   rag-ingest-mcp fetches the JSON out of band and, seeing no vectors
   (chunk-only), embeds locally with `text-embedding-3-small`
5. `search("main revenue drivers", collection="reports")` → answers from the
   top chunks

Loading the same file again **upserts** (element ids are deterministic), so
re-running a pipeline never duplicates rows. To wipe a corpus, delete
`URAG_CHROMA_PATH` — there is no service to reset.

## 4. Inspect what landed

Chroma is embedded, so inspection is a Python one-liner against the same path
(stop your MCP host first, or copy the directory — Chroma allows one writer):

```bash
python -c "
import chromadb
c = chromadb.PersistentClient(path='$HOME/.unstructured-rag/chroma')
for item in c.list_collections():
    coll = c.get_collection(item.name if hasattr(item, 'name') else item)
    print(coll.name, coll.count(), coll.metadata)   # metadata = the pinned embedding space
    rows = coll.peek(3)
    for id, doc in zip(rows['ids'], rows['documents']):
        print('  ', id, '|', doc[:70])
"
```

The storage is SQLite underneath, so raw poking works too:

```bash
sqlite3 ~/.unstructured-rag/chroma/chroma.sqlite3 \
  "SELECT name FROM sqlite_master WHERE type='table';"
```

## Pairing with Chroma's own MCP server

The official [`chroma-mcp`](https://github.com/chroma-core/chroma-mcp) can point
at the same directory, and it's handy for admin-style operations (listing,
counting, deleting collections). **Don't use it for querying this corpus,
though:** its `query` embeds with the collection's persisted embedding function,
and collections written by rag-ingest-mcp (like those written by the ingest
connector) have none — chroma-mcp would fall back to its default local model,
which is a different embedding space than your corpus. Same collection, wrong
vectors, quietly wrong results. `search` on rag-ingest-mcp reads the pinned
space and embeds the query to match — that's the reliable query path.

## Chroma-specific notes

- The embedding-space pin lives in the collection's metadata (visible in the
  inspection snippet above, alongside `hnsw:space: cosine`).
- One process at a time: Chroma's persistent client locks the directory. If an
  inspection script hangs, your MCP host's rag-ingest-mcp subprocess probably
  has it open.
- There is no dimension ceiling to worry about — any embedding model's width
  works.
