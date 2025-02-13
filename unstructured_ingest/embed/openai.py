from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
    EMBEDDINGS_KEY,
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.errors import (
    ProviderError,
    QuotaError,
    RateLimitError,
    UserAuthError,
    UserError,
)

if TYPE_CHECKING:
    from openai import AsyncOpenAI, OpenAI


class OpenAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(default="text-embedding-ada-002", alias="model_name")

    def wrap_error(self, e: Exception) -> Exception:
        # https://platform.openai.com/docs/guides/error-codes/api-errors
        from openai import APIStatusError

        if not isinstance(e, APIStatusError):
            logger.error(f"unhandled exception from openai: {e}", exc_info=True)
            raise e
        error_code = e.code
        if 400 <= e.status_code < 500:
            # user error
            if e.status_code == 401:
                return UserAuthError(e.message)
            if e.status_code == 429:
                # 429 indicates rate limit exceeded and quote exceeded
                if error_code == "insufficient_quota":
                    return QuotaError(e.message)
                else:
                    return RateLimitError(e.message)
            return UserError(e.message)
        if e.status_code >= 500:
            return ProviderError(e.message)
        logger.error(f"unhandled exception from openai: {e}", exc_info=True)
        return e

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

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def embed_query(self, query: str) -> list[float]:

        client = self.config.get_client()
        try:
            response = client.embeddings.create(input=query, model=self.config.embedder_model_name)
        except Exception as e:
            raise self.wrap_error(e=e)
        return response.data[0].embedding

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        client = self.config.get_client()
        elements = elements.copy()
        elements_with_text = [e for e in elements if e.get("text")]
        texts = [e["text"] for e in elements_with_text]
        embeddings = []
        try:
            for batch in batch_generator(texts, batch_size=self.config.batch_size or len(texts)):
                response = client.embeddings.create(
                    input=batch, model=self.config.embedder_model_name
                )
                embeddings.extend([data.embedding for data in response.data])
        except Exception as e:
            raise self.wrap_error(e=e)
        for element, embedding in zip(elements_with_text, embeddings):
            element[EMBEDDINGS_KEY] = embedding
        return elements


@dataclass
class AsyncOpenAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: OpenAIEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    async def embed_query(self, query: str) -> list[float]:
        client = self.config.get_async_client()
        try:
            response = await client.embeddings.create(
                input=query, model=self.config.embedder_model_name
            )
        except Exception as e:
            raise self.wrap_error(e=e)
        return response.data[0].embedding

    async def embed_documents(self, elements: list[dict]) -> list[dict]:
        client = self.config.get_async_client()
        elements = elements.copy()
        elements_with_text = [e for e in elements if e.get("text")]
        texts = [e["text"] for e in elements_with_text]
        embeddings = []
        try:
            for batch in batch_generator(texts, batch_size=self.config.batch_size or len(texts)):
                response = await client.embeddings.create(
                    input=batch, model=self.config.embedder_model_name
                )
                embeddings.extend([data.embedding for data in response.data])
        except Exception as e:
            raise self.wrap_error(e=e)
        for element, embedding in zip(elements_with_text, embeddings):
            element[EMBEDDINGS_KEY] = embedding
        return elements
