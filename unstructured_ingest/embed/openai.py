from dataclasses import dataclass
from typing import TYPE_CHECKING, List

import numpy as np
from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from langchain_openai.embeddings import OpenAIEmbeddings


class OpenAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(default="text-embedding-ada-002", alias="model_name")

    @requires_dependencies(["langchain_openai"], extras="openai")
    def get_client(self) -> "OpenAIEmbeddings":
        """Creates a langchain OpenAI python client to embed elements."""
        from langchain_openai import OpenAIEmbeddings

        openai_client = OpenAIEmbeddings(
            openai_api_key=self.api_key.get_secret_value(),
            model=self.embedder_model_name,  # type:ignore
        )
        return openai_client


@dataclass
class OpenAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: OpenAIEmbeddingConfig

    def get_exemplary_embedding(self) -> List[float]:
        return self.embed_query(query="Q")

    def num_of_dimensions(self):
        exemplary_embedding = self.get_exemplary_embedding()
        return np.shape(exemplary_embedding)

    def is_unit_vector(self):
        exemplary_embedding = self.get_exemplary_embedding()
        return np.isclose(np.linalg.norm(exemplary_embedding), 1.0)

    def embed_query(self, query):
        client = self.config.get_client()
        return client.embed_query(str(query))

    def embed_documents(self, elements: List[dict]) -> List[dict]:
        client = self.config.get_client()
        embeddings = client.embed_documents([e.get("text", "") for e in elements])
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings

    def _add_embeddings_to_elements(self, elements, embeddings) -> List[dict]:
        assert len(elements) == len(embeddings)
        elements_w_embedding = []
        for i, element in enumerate(elements):
            element["embeddings"] = embeddings[i]
            elements_w_embedding.append(element)
        return elements
