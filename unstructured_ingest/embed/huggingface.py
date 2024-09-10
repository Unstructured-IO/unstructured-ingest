from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

import numpy as np
from pydantic import Field

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from langchain_huggingface.embeddings import HuggingFaceEmbeddings


class HuggingFaceEmbeddingConfig(EmbeddingConfig):
    embedder_model_name: Optional[str] = Field(
        default="sentence-transformers/all-MiniLM-L6-v2", alias="model_name"
    )
    embedder_model_kwargs: Optional[dict] = Field(
        default_factory=lambda: {"device": "cpu"}, alias="model_kwargs"
    )
    encode_kwargs: Optional[dict] = Field(default_factory=lambda: {"normalize_embeddings": False})
    cache_folder: Optional[dict] = Field(default=None)

    @requires_dependencies(
        ["langchain_huggingface"],
        extras="embed-huggingface",
    )
    def get_client(self) -> "HuggingFaceEmbeddings":
        """Creates a langchain Huggingface python client to embed elements."""
        from langchain_huggingface.embeddings import HuggingFaceEmbeddings

        client = HuggingFaceEmbeddings(
            model_name=self.embedder_model_name,
            model_kwargs=self.embedder_model_kwargs,
            encode_kwargs=self.encode_kwargs,
            cache_folder=self.cache_folder,
        )
        return client


@dataclass
class HuggingFaceEmbeddingEncoder(BaseEmbeddingEncoder):
    config: HuggingFaceEmbeddingConfig

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

    def _add_embeddings_to_elements(self, elements: list[dict], embeddings: list) -> List[dict]:
        assert len(elements) == len(embeddings)
        elements_w_embedding = []

        for i, element in enumerate(elements):
            element["embeddings"] = embeddings[i]
            elements_w_embedding.append(element)
        return elements
