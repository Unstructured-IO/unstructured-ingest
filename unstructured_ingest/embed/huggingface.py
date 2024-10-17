from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from pydantic import Field

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


class HuggingFaceEmbeddingConfig(EmbeddingConfig):
    embedder_model_name: Optional[str] = Field(
        default="sentence-transformers/all-MiniLM-L6-v2", alias="model_name"
    )
    embedder_model_kwargs: Optional[dict] = Field(
        default_factory=lambda: {"device": "cpu"}, alias="model_kwargs"
    )
    encode_kwargs: Optional[dict] = Field(default_factory=lambda: {"normalize_embeddings": False})
    cache_folder: Optional[str] = Field(default=None)

    @requires_dependencies(
        ["sentence_transformers"],
        extras="embed-huggingface",
    )
    def get_client(self) -> "SentenceTransformer":
        from sentence_transformers import SentenceTransformer

        return SentenceTransformer(
            model_name_or_path=self.embedder_model_name,
            cache_folder=self.cache_folder,
            **self.embedder_model_kwargs,
        )


@dataclass
class HuggingFaceEmbeddingEncoder(BaseEmbeddingEncoder):
    config: HuggingFaceEmbeddingConfig

    def embed_query(self, query: str) -> list[float]:
        return self._embed_documents(texts=[query])[0]

    def _embed_documents(self, texts: list[str]) -> list[list[float]]:
        client = self.config.get_client()
        embeddings = client.encode(texts, **self.config.encode_kwargs)
        return embeddings.tolist()

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings
