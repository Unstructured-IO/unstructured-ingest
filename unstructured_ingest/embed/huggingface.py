from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from pydantic import Field

from unstructured_ingest.embed.interfaces import (
    EMBEDDINGS_KEY,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
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

    def get_encoder_kwargs(self) -> dict:
        encoder_kwargs = self.encode_kwargs or {}
        encoder_kwargs["batch_size"] = self.batch_size
        return encoder_kwargs


@dataclass
class HuggingFaceEmbeddingEncoder(BaseEmbeddingEncoder):
    config: HuggingFaceEmbeddingConfig

    def _embed_query(self, query: str) -> list[float]:
        return self._embed_documents(texts=[query])[0]

    def _embed_documents(self, texts: list[str]) -> list[list[float]]:
        client = self.config.get_client()
        embeddings = client.encode(texts, **self.config.get_encoder_kwargs())
        return embeddings.tolist()

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        elements = elements.copy()
        elements_with_text = [e for e in elements if e.get("text")]
        if not elements_with_text:
            return elements
        embeddings = self._embed_documents([e["text"] for e in elements_with_text])
        for element, embedding in zip(elements_with_text, embeddings):
            element[EMBEDDINGS_KEY] = embedding
        return elements
