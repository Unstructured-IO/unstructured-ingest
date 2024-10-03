from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

import numpy as np
from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from voyageai import Client as VoyageAIClient


class VoyageAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(alias="model_name")
    batch_size: Optional[int] = Field(default=None)
    truncation: Optional[bool] = Field(default=None)
    max_retries: int = 0
    timeout_in_seconds: Optional[int] = None

    @requires_dependencies(
        ["langchain", "langchain_voyageai"],
        extras="embed-voyageai",
    )
    def get_client(self) -> "VoyageAIClient":
        """Creates a Langchain VoyageAI python client to embed elements."""
        from voyageai import Client as VoyageAIClient

        client = VoyageAIClient(
            api_key=self.api_key.get_secret_value(),
            max_retries=self.max_retries,
            timeout=self.timeout_in_seconds,
        )
        return client


@dataclass
class VoyageAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: VoyageAIEmbeddingConfig

    def get_exemplary_embedding(self) -> list[float]:
        return self.embed_query(query="A sample query.")

    @property
    def num_of_dimensions(self) -> tuple[int, ...]:
        exemplary_embedding = self.get_exemplary_embedding()
        return np.shape(exemplary_embedding)

    @property
    def is_unit_vector(self) -> bool:
        exemplary_embedding = self.get_exemplary_embedding()
        return np.isclose(np.linalg.norm(exemplary_embedding), 1.0)

    def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        client: VoyageAIClient = self.config.get_client()
        response = client.embed(texts=elements, model=self.config.embedder_model_name)
        return response.embeddings

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        return self._add_embeddings_to_elements(elements, embeddings)

    def embed_query(self, query: str) -> list[float]:
        return self._embed_documents(elements=[query])[0]

    @staticmethod
    def _add_embeddings_to_elements(elements, embeddings) -> list[dict]:
        assert len(elements) == len(embeddings)
        elements_w_embedding = []
        for i, element in enumerate(elements):
            element["embeddings"] = embeddings[i]
            elements_w_embedding.append(element)
        return elements
