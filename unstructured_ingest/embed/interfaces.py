from abc import ABC, abstractmethod
from dataclasses import dataclass

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
    @abstractmethod
    def num_of_dimensions(self) -> tuple[int, ...]:
        """Number of dimensions for the embedding vector."""

    @property
    @abstractmethod
    def is_unit_vector(self) -> bool:
        """Denotes if the embedding vector is a unit vector."""

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
