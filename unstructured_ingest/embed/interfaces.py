from abc import ABC
from dataclasses import dataclass
from typing import Any, Optional

from pydantic import BaseModel, Field

from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies

EMBEDDINGS_KEY = "embeddings"


class EmbeddingConfig(BaseModel):
    batch_size: Optional[int] = Field(
        default=32, description="Optional batch size for embedding requests."
    )


@dataclass
class BaseEncoder(ABC):
    config: EmbeddingConfig

    def precheck(self):
        pass

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
    @requires_dependencies(["numpy"])
    def is_unit_vector(self) -> bool:
        """Denotes if the embedding vector is a unit vector."""
        import numpy as np

        exemplary_embedding = self.get_exemplary_embedding()
        return np.isclose(np.linalg.norm(exemplary_embedding), 1.0, rtol=1e-03)

    def get_client(self):
        raise NotImplementedError

    def embed_batch(self, client: Any, batch: list[str]) -> list[list[float]]:
        raise NotImplementedError

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        client = self.get_client()
        elements = elements.copy()
        elements_with_text = [e for e in elements if e.get("text")]
        texts = [e["text"] for e in elements_with_text]
        embeddings = []
        try:
            for batch in batch_generator(texts, batch_size=self.config.batch_size or len(texts)):
                embeddings = self.embed_batch(client=client, batch=batch)
                embeddings.extend(embeddings)
        except Exception as e:
            raise self.wrap_error(e=e)
        for element, embedding in zip(elements_with_text, embeddings):
            element[EMBEDDINGS_KEY] = embedding
        return elements

    def _embed_query(self, query: str) -> list[float]:
        client = self.get_client()
        return self.embed_batch(client=client, batch=[query])[0]

    def embed_query(self, query: str) -> list[float]:
        try:
            return self._embed_query(query=query)
        except Exception as e:
            raise self.wrap_error(e=e)


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
    @requires_dependencies(["numpy"])
    async def is_unit_vector(self) -> bool:
        """Denotes if the embedding vector is a unit vector."""
        import numpy as np

        exemplary_embedding = await self.get_exemplary_embedding()
        return np.isclose(np.linalg.norm(exemplary_embedding), 1.0, rtol=1e-03)

    def get_client(self):
        raise NotImplementedError

    async def embed_batch(self, client: Any, batch: list[str]) -> list[list[float]]:
        raise NotImplementedError

    async def embed_documents(self, elements: list[dict]) -> list[dict]:
        client = self.get_client()
        elements = elements.copy()
        elements_with_text = [e for e in elements if e.get("text")]
        texts = [e["text"] for e in elements_with_text]
        embeddings = []
        try:
            for batch in batch_generator(texts, batch_size=self.config.batch_size or len(texts)):
                embeddings = await self.embed_batch(client=client, batch=batch)
                embeddings.extend(embeddings)
        except Exception as e:
            raise self.wrap_error(e=e)
        for element, embedding in zip(elements_with_text, embeddings):
            element[EMBEDDINGS_KEY] = embedding
        return elements

    async def _embed_query(self, query: str) -> list[float]:
        client = self.get_client()
        embeddings = await self.embed_batch(client=client, batch=[query])
        return embeddings[0]

    async def embed_query(self, query: str) -> list[float]:
        try:
            return await self._embed_query(query=query)
        except Exception as e:
            raise self.wrap_error(e=e)
