from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

import numpy as np
from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from openai import OpenAI


class OctoAiEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(default="thenlper/gte-large", alias="model_name")
    base_url: str = Field(default="https://text.octoai.run/v1")

    @requires_dependencies(
        ["openai", "tiktoken"],
        extras="embed-octoai",
    )
    def get_client(self) -> "OpenAI":
        """Creates an OpenAI python client to embed elements. Uses the OpenAI SDK."""
        from openai import OpenAI

        return OpenAI(api_key=self.api_key.get_secret_value(), base_url=self.base_url)


@dataclass
class OctoAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: OctoAiEmbeddingConfig
    # Uses the OpenAI SDK
    _exemplary_embedding: Optional[list[float]] = field(init=False, default=None)

    def get_exemplary_embedding(self) -> list[float]:
        return self.embed_query("Q")

    def num_of_dimensions(self) -> tuple[int, ...]:
        exemplary_embedding = self.get_exemplary_embedding()
        return np.shape(exemplary_embedding)

    def is_unit_vector(self) -> bool:
        exemplary_embedding = self.get_exemplary_embedding()
        return np.isclose(np.linalg.norm(exemplary_embedding), 1.0)

    def embed_query(self, query: str):
        client = self.config.get_client()
        response = client.embeddings.create(input=query, model=self.config.embedder_model_name)
        return response.data[0].embedding

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = [self.embed_query(e.get("text", "")) for e in elements]
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings

    def _add_embeddings_to_elements(self, elements, embeddings) -> list[dict]:
        assert len(elements) == len(embeddings)
        elements_w_embedding = []
        for i, element in enumerate(elements):
            element["embeddings"] = embeddings[i]
            elements_w_embedding.append(element)
        return elements
