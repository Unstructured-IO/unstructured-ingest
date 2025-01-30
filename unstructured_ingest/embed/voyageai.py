from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

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
    UserAuthError,
    UserError,
)
from unstructured_ingest.v2.errors import (
    RateLimitError as CustomRateLimitError,
)

if TYPE_CHECKING:
    from voyageai import AsyncClient as AsyncVoyageAIClient
    from voyageai import Client as VoyageAIClient


class VoyageAIEmbeddingConfig(EmbeddingConfig):
    batch_size: int = Field(
        default=32,
        le=128,
        description="Batch size for embedding requests. VoyageAI has a limit of 128.",
    )
    api_key: SecretStr
    embedder_model_name: str = Field(default="voyage-3", alias="model_name")
    truncation: Optional[bool] = Field(default=None)
    max_retries: int = 0
    timeout_in_seconds: Optional[int] = None

    def wrap_error(self, e: Exception) -> Exception:
        # https://docs.voyageai.com/docs/error-codes
        from voyageai.error import AuthenticationError, RateLimitError, VoyageError

        if not isinstance(e, VoyageError):
            logger.error(f"unhandled exception from openai: {e}", exc_info=True)
            raise e
        http_code = e.http_status
        message = e.user_message
        if isinstance(e, AuthenticationError):
            return UserAuthError(message)
        if isinstance(e, RateLimitError):
            return CustomRateLimitError(message)
        if 400 <= http_code < 500:
            return UserError(message)
        if http_code >= 500:
            return ProviderError(message)
        logger.error(f"unhandled exception from openai: {e}", exc_info=True)
        return e

    @requires_dependencies(
        ["voyageai"],
        extras="embed-voyageai",
    )
    def get_client(self) -> "VoyageAIClient":
        """Creates a VoyageAI python client to embed elements."""
        from voyageai import Client as VoyageAIClient

        client = VoyageAIClient(
            api_key=self.api_key.get_secret_value(),
            max_retries=self.max_retries,
            timeout=self.timeout_in_seconds,
        )
        return client

    @requires_dependencies(
        ["voyageai"],
        extras="embed-voyageai",
    )
    def get_async_client(self) -> "AsyncVoyageAIClient":
        """Creates a VoyageAI python client to embed elements."""
        from voyageai import AsyncClient as AsyncVoyageAIClient

        client = AsyncVoyageAIClient(
            api_key=self.api_key.get_secret_value(),
            max_retries=self.max_retries,
            timeout=self.timeout_in_seconds,
        )
        return client


@dataclass
class VoyageAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: VoyageAIEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        client = self.config.get_client()
        embeddings = []
        try:
            for batch in batch_generator(elements, batch_size=self.config.batch_size):
                response = client.embed(texts=batch, model=self.config.embedder_model_name)
                embeddings.extend(response.embeddings)
        except Exception as e:
            raise self.wrap_error(e=e)
        return embeddings

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        return self._add_embeddings_to_elements(elements, embeddings)

    def embed_query(self, query: str) -> list[float]:
        return self._embed_documents(elements=[query])[0]


@dataclass
class AsyncVoyageAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: VoyageAIEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    async def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        client = self.config.get_async_client()
        embeddings = []
        try:
            for batch in batch_generator(
                elements, batch_size=self.config.batch_size or len(elements)
            ):
                response = await client.embed(texts=batch, model=self.config.embedder_model_name)
                embeddings.extend(response.embeddings)
        except Exception as e:
            raise self.wrap_error(e=e)
        return embeddings

    async def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = await self._embed_documents([e.get("text", "") for e in elements])
        return self._add_embeddings_to_elements(elements, embeddings)

    async def embed_query(self, query: str) -> list[float]:
        embedding = await self._embed_documents(elements=[query])
        return embedding[0]
