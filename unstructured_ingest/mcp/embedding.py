"""Embedding encoders for the ingest RAG MCP server.

Both sides of the system embed through this one builder: the ingest side (when
Transform did not embed) and the query side (always). Routing both through the
same provider/model is what guarantees a query vector lands in the same space as
the corpus it searches — the single property retrieval correctness depends on.

Only providers whose key this server can supply locally belong in the registry.
Adding one is a single row plus its ``unstructured-ingest`` extra.
"""

from __future__ import annotations

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder
from unstructured_ingest.embed.openai import OpenAIEmbeddingConfig, OpenAIEmbeddingEncoder

# provider -> (config class, encoder class). The config's ``model_name`` alias
# and ``api_key`` field are shared across the ingest encoders, so one call shape
# fits every provider added here.
_PROVIDERS: dict[str, tuple[type, type]] = {
    "openai": (OpenAIEmbeddingConfig, OpenAIEmbeddingEncoder),
}


def build_encoder(
    *,
    provider: str,
    model: str,
    api_key: str | None,
    base_url: str | None = None,
    batch_size: int = 64,
) -> BaseEmbeddingEncoder:
    """Construct an ingest embedding encoder for ``provider``/``model``.

    Raises ``ValueError`` — rather than failing deep inside a request — when the
    provider is unsupported or its key is absent, so the tool can report a clear
    remedy.
    """
    if provider not in _PROVIDERS:
        raise ValueError(
            f"unsupported embed provider {provider!r}; supported: {sorted(_PROVIDERS)}"
        )
    if not api_key:
        raise ValueError(
            f"no API key configured for embed provider {provider!r}; set OPENAI_API_KEY"
        )
    config_cls, encoder_cls = _PROVIDERS[provider]
    # ``model_name`` is the public alias of the encoder config's model field.
    config = config_cls(api_key=api_key, model_name=model, base_url=base_url, batch_size=batch_size)
    return encoder_cls(config=config)
