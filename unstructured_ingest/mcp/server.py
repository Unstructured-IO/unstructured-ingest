"""FastMCP STDIO server: land Transform MCP output into a local vector store.

Runs as a local subprocess of the MCP host (Claude Desktop / Claude Code)
alongside the remote Transform MCP. The agent parses/chunks/(optionally)embeds
via Transform, then calls ``load_transform_output`` here with a download_url;
this server fetches the Element JSON out of band, embeds it locally iff Transform
did not, and upserts it into a local vector store — Chroma by default, Qdrant or
pgvector via ``URAG_STORE_BACKEND``. ``search`` then embeds a query into that
collection's own space and returns the nearest chunks.

The embedding step is the only knob: Transform embeds (passthrough) OR this
server embeds (local), selected per load. Either way a collection records exactly
one embedding space and every query is embedded into it, so corpus and query
vectors are always comparable.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from fastmcp import FastMCP

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.mcp.config import Config
from unstructured_ingest.mcp.embedding import build_encoder
from unstructured_ingest.mcp.fetch import fetch_elements
from unstructured_ingest.mcp.stores import (
    EmbeddingSpace,
    SpaceMismatch,
    VectorStore,
    build_store,
)

mcp = FastMCP("rag-ingest-mcp")

_VALID_POLICIES = ("auto", "passthrough", "local")


def _store(cfg: Config) -> VectorStore:
    return build_store(cfg)


def _source_identity(source: str) -> str:
    """A stable per-document id so re-loading the same source upserts.

    A Transform download URL's last path segment is the deterministic artifact id
    (job + file + format), which makes a stable key; a local path uses its name.
    """
    name = Path(urlparse(source).path).name
    return name or "transform-output"


def _expand_sources(sources: list[str]) -> tuple[list[str], list[dict]]:
    """Expand any local directory into the .json files under it; pass others through.

    Transform MCP output is commonly a directory of per-document Element JSON, so
    pointing this tool at that directory ingests every ``*.json`` under it
    (recursively). URLs and individual file paths pass through unchanged. Returns
    the work list plus per-source errors for directories that held no JSON.
    """
    work: list[str] = []
    errors: list[dict] = []
    for source in sources:
        parsed = urlparse(source)
        if parsed.scheme in ("", "file"):
            path = Path(parsed.path if parsed.scheme == "file" else source)
            if path.is_dir():
                found = sorted(str(p) for p in path.rglob("*.json"))
                if found:
                    work.extend(found)
                else:
                    errors.append({"source": source, "error": "no .json files found in directory"})
                continue
        work.append(source)
    return work, errors


@mcp.tool
def load_transform_output(
    sources: list[str],
    collection: str,
    embed_policy: str = "auto",
    embed_model: str | None = None,
) -> dict:
    """Load Transform MCP Element JSON into a local vector-store collection.

    Fetches each source out of band (the JSON never enters the agent context),
    embeds locally when needed, and upserts into ``collection``.

    Args:
        sources: One or more ``download_url``s from ``get_transform_results``
            (call it with ``output_format="json"``), local file paths, or a
            local directory — a directory is expanded to every ``*.json`` under
            it, so you can point this at a whole Transform output folder.
        collection: Target collection. Created on first load and pinned to the
            embedding space used; later loads with a different model are refused.
        embed_policy: ``"auto"`` (default) embeds locally only when elements
            arrive without vectors; ``"passthrough"`` requires Transform-supplied
            vectors; ``"local"`` always (re-)embeds here.
        embed_model: Override the embed model. In ``passthrough`` this records
            which model Transform used (defaults to the server's configured
            model); in ``local`` it selects the model to embed with.

    Returns:
        A summary: the collection, the policy, the pinned embedding space, and a
        per-source count of rows loaded. Errors are reported per source, not
        raised, so one bad URL doesn't sink a batch.
    """
    if embed_policy not in _VALID_POLICIES:
        return {
            "error": f"embed_policy must be one of {list(_VALID_POLICIES)}; got {embed_policy!r}"
        }

    cfg = Config.from_env()
    try:
        store = _store(cfg)
    except ValueError as exc:
        return {"error": str(exc)}

    model = embed_model or cfg.embed_model
    encoder = None  # built lazily and reused across sources that embed locally
    pinned_space: EmbeddingSpace | None = None

    work_items, files = _expand_sources(sources)

    for source in work_items:
        try:
            elements = fetch_elements(source, max_bytes=cfg.max_fetch_bytes)
        except Exception as exc:
            files.append({"source": source, "error": str(exc)})
            continue

        has_vectors = any(el.get("embeddings") for el in elements)
        embed_here = embed_policy == "local" or (embed_policy == "auto" and not has_vectors)

        if embed_here:
            try:
                if encoder is None:
                    encoder = build_encoder(
                        provider=cfg.embed_provider,
                        model=model,
                        api_key=cfg.openai_api_key,
                        base_url=cfg.openai_base_url,
                        batch_size=cfg.embed_batch_size,
                    )
                elements = encoder.embed_documents(elements)
            except Exception as exc:
                files.append({"source": source, "error": f"local embedding failed: {exc}"})
                continue
            space = EmbeddingSpace(cfg.embed_provider, model, encoder.dimension)
        else:
            first_vector = next((el["embeddings"] for el in elements if el.get("embeddings")), None)
            if first_vector is None:
                files.append(
                    {
                        "source": source,
                        "error": "no vectors present; retry this source with embed_policy='local'",
                    }
                )
                continue
            space = EmbeddingSpace(cfg.embed_provider, model, len(first_vector))

        try:
            store.ensure_space(collection, space)
        except SpaceMismatch as exc:
            files.append({"source": source, "error": str(exc)})
            continue

        file_data = FileData(
            identifier=_source_identity(source),
            connector_type="transform_mcp",
            source_identifiers=SourceIdentifiers(
                filename=_source_identity(source), fullpath=source
            ),
        )
        loaded = store.write(collection, elements, file_data)
        pinned_space = space
        files.append({"source": source, "loaded": loaded, "elements": len(elements)})

    return {
        "collection": collection,
        "embed_policy": embed_policy,
        "space": pinned_space.as_metadata() if pinned_space else None,
        "files": files,
    }


@mcp.tool
def search(query: str, collection: str, k: int = 5) -> dict:
    """Similarity-search a collection, embedding the query in its own space.

    Reads the collection's pinned (provider, model) and embeds ``query`` with the
    same one, so the query and the corpus are always comparable — the reason a
    passthrough (Transform-embedded) collection still needs the matching provider
    key available to this server.

    Args:
        query: The natural-language query text.
        collection: A collection previously built by ``load_transform_output``.
        k: Number of nearest chunks to return.

    Returns:
        The matches (each with a cosine ``score``, ``text``, and ``metadata``),
        plus the model they were compared in.
    """
    cfg = Config.from_env()
    try:
        store = _store(cfg)
        space = store.space_of(collection)
    except Exception as exc:
        return {"error": f"unknown or unbuilt collection {collection!r}: {exc}"}

    try:
        encoder = build_encoder(
            provider=space.provider or cfg.embed_provider,
            model=space.model,
            api_key=cfg.openai_api_key,
            base_url=cfg.openai_base_url,
            batch_size=cfg.embed_batch_size,
        )
        query_vector = encoder.embed_query(query)
    except Exception as exc:
        return {"error": f"could not embed query in the collection's space: {exc}"}

    return {
        "collection": collection,
        "model": space.model,
        "matches": store.search(collection, query_vector, k),
    }


@mcp.tool
def list_collections() -> dict:
    """List local collections and the embedding space each is pinned to."""
    cfg = Config.from_env()
    try:
        store = _store(cfg)
    except ValueError as exc:
        return {"error": str(exc)}
    out = []
    for name in store.collections():
        try:
            space = store.space_of(name)
            out.append({"collection": name, "model": space.model, "dimension": space.dimension})
        except Exception:
            out.append({"collection": name})
    return {"collections": out}


def main() -> None:
    """Run the server over STDIO (the transport MCP hosts launch it with)."""
    mcp.run()


if __name__ == "__main__":
    main()
