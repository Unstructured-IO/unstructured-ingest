from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

import numpy as np
from pydantic import Field

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from ollama import embed as OllamaClient


class OllamaEmbeddingConfig(EmbeddingConfig):
    embedder_model_name: Optional[str] = Field(default="all-minilm", alias="model_name")

    @requires_dependencies(
        ["ollama"],
        extras="embed-ollama",
    )
    def get_client(self) -> "OllamaClient":
        from ollama import embed as OllamaClient

        return OllamaClient


@dataclass
class OllamaEmbeddingEncoder(BaseEmbeddingEncoder):
    config: OllamaEmbeddingConfig

    def get_exemplary_embedding(self) -> list[float]:
        return self.embed_query(query="Q")

    def num_of_dimensions(self) -> tuple[int, ...]:
        exemplary_embedding = self.get_exemplary_embedding()
        return np.shape(exemplary_embedding)

    def is_unit_vector(self) -> bool:
        exemplary_embedding = self.get_exemplary_embedding()
        return np.isclose(np.linalg.norm(exemplary_embedding), 1.0)

    def embed_query(self, query: str) -> list[float]:
        return self._embed_documents(texts=[query])[0]

    def _embed_documents(self, texts: list[str]) -> list[list[float]]:
        client = self.config.get_client()
        _r = client(model=self.config.embedder_model_name, input=texts)
        return _r['embeddings']

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings

    def _add_embeddings_to_elements(self, elements: list[dict], embeddings: list) -> list[dict]:
        assert len(elements) == len(embeddings)
        elements_w_embedding = []

        for i, element in enumerate(elements):
            element["embeddings"] = embeddings[i]
            elements_w_embedding.append(element)
        return elements
