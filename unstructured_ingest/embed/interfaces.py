from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np
from pydantic import BaseModel


class EmbeddingConfig(BaseModel):
    pass


@dataclass
class BaseEmbeddingEncoder(ABC):
    config: EmbeddingConfig

    def initialize(self):
        """Initializes the embedding encoder class. Should also validate the instance
        is properly configured: e.g., embed a single a element"""

    @property
    def num_of_dimensions(self) -> tuple[int, ...]:
        exemplary_embedding = self.get_exemplary_embedding()
        return np.shape(exemplary_embedding)

    def get_exemplary_embedding(self) -> list[float]:
        return self.embed_query(query="Q")

    @property
    def is_unit_vector(self) -> bool:
        """Denotes if the embedding vector is a unit vector."""
        exemplary_embedding = self.get_exemplary_embedding()
        return np.isclose(np.linalg.norm(exemplary_embedding), 1.0)

    @abstractmethod
    def embed_documents(self, elements: list[dict]) -> list[dict]:
        pass

    @abstractmethod
    def embed_query(self, query: str) -> list[float]:
        pass

    def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        results = []
        for text in elements:
            response = self.embed_query(query=text)
            results.append(response)

        return results

    @staticmethod
    def _add_embeddings_to_elements(
        elements: list[dict], embeddings: list[list[float]]
    ) -> list[dict]:
        """
        Add embeddings to elements.

        Args:
            elements (list[Element]): List of elements.
            embeddings (list[list[float]]): List of embeddings.

        Returns:
            list[Element]: Elements with embeddings added.
        """
        assert len(elements) == len(embeddings)
        elements_w_embedding = []
        for i, element in enumerate(elements):
            element["embeddings"] = embeddings[i]
            elements_w_embedding.append(element)
        return elements
