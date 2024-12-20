from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from openai import AsyncOpenAI, OpenAI


class OpenAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(default="text-embedding-ada-002", alias="model_name")

    @requires_dependencies(["openai"], extras="openai")
    def get_client(self) -> "OpenAI":
        from openai import OpenAI

        return OpenAI(api_key=self.api_key.get_secret_value())

    @requires_dependencies(["openai"], extras="openai")
    def get_async_client(self) -> "AsyncOpenAI":
        from openai import AsyncOpenAI

        return AsyncOpenAI(api_key=self.api_key.get_secret_value())


@dataclass
class OpenAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: OpenAIEmbeddingConfig

    def embed_query(self, query: str) -> list[float]:
        client = self.config.get_client()
        response = client.embeddings.create(input=query, model=self.config.embedder_model_name)
        return response.data[0].embedding

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings


@dataclass
class AsyncOpenAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: OpenAIEmbeddingConfig

    async def embed_query(self, query: str) -> list[float]:
        client = self.config.get_async_client()
        response = await client.embeddings.create(
            input=query, model=self.config.embedder_model_name
        )
        return response.data[0].embedding

    async def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = await self._embed_documents([e.get("text", "") for e in elements])
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings
