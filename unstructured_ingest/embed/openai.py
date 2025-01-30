from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
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
        texts = [e.get("text", "") for e in elements]
        embeddings = []
        try:
            for batch in batch_generator(texts, batch_size=self.config.batch_size or len(texts)):
                response = client.embeddings.create(
                    input=batch, model=self.config.embedder_model_name
                )
                embeddings.extend([data.embedding for data in response.data])
        except Exception as e:
            raise self.wrap_error(e=e)
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings


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
        texts = [e.get("text", "") for e in elements]
        embeddings = []
        try:
            for batch in batch_generator(texts, batch_size=self.config.batch_size or len(texts)):
                response = await client.embeddings.create(
                    input=batch, model=self.config.embedder_model_name
                )
                embeddings.extend([data.embedding for data in response.data])
        except Exception as e:
            raise self.wrap_error(e=e)
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings
