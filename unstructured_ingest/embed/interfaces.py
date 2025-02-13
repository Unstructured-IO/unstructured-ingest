import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

import numpy as np
from pydantic import BaseModel, Field

EMBEDDINGS_KEY = "embeddings"


class EmbeddingConfig(BaseModel):
    batch_size: Optional[int] = Field(
        default=32, description="Optional batch size for embedding requests."
    )


@dataclass
class BaseEncoder(ABC):
    config: EmbeddingConfig

    def initialize(self):
        """Initializes the embedding encoder class. Should also validate the instance
        is properly configured: e.g., embed a single a element"""

    def wrap_error(self, e: Exception) -> Exception:
        """Handle errors from the embedding service. Should raise a more informative error
        if possible"""
        return e


@dataclass
class BaseEmbeddingEncoder(BaseEncoder, ABC):

    def initialize(self):
        """Initializes the embedding encoder class. Should also validate the instance
        is properly configured: e.g., embed a single a element"""

    @property
    def dimension(self):
        exemplary_embedding = self.get_exemplary_embedding()
        return len(exemplary_embedding)

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


@dataclass
class AsyncBaseEmbeddingEncoder(BaseEncoder, ABC):

    async def initialize(self):
        """Initializes the embedding encoder class. Should also validate the instance
        is properly configured: e.g., embed a single a element"""

    @property
    async def dimension(self):
        exemplary_embedding = await self.get_exemplary_embedding()
        return len(exemplary_embedding)

    async def get_exemplary_embedding(self) -> list[float]:
        return await self.embed_query(query="Q")

    @property
    async def is_unit_vector(self) -> bool:
        """Denotes if the embedding vector is a unit vector."""
        exemplary_embedding = await self.get_exemplary_embedding()
        return np.isclose(np.linalg.norm(exemplary_embedding), 1.0)

    @abstractmethod
    async def embed_documents(self, elements: list[dict]) -> list[dict]:
        pass

    @abstractmethod
    async def embed_query(self, query: str) -> list[float]:
        pass

    async def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        results = await asyncio.gather(*[self.embed_query(query=text) for text in elements])
        return results
