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
    RateLimitError as CustomRateLimitError,
)
from unstructured_ingest.v2.errors import (
    UserAuthError,
    UserError,
)

if TYPE_CHECKING:
    from together import AsyncTogether, Together


class TogetherAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(
        default="togethercomputer/m2-bert-80M-8k-retrieval", alias="model_name"
    )

    def wrap_error(self, e: Exception) -> Exception:
        # https://docs.together.ai/docs/error-codes
        from together.error import AuthenticationError, RateLimitError, TogetherException

        if not isinstance(e, TogetherException):
            logger.error(f"unhandled exception from openai: {e}", exc_info=True)
            return e
        message = e.args[0]
        if isinstance(e, AuthenticationError):
            return UserAuthError(message)
        if isinstance(e, RateLimitError):
            return CustomRateLimitError(message)
        return UserError(message)

    @requires_dependencies(["together"], extras="togetherai")
    def get_client(self) -> "Together":
        from together import Together

        return Together(api_key=self.api_key.get_secret_value())

    @requires_dependencies(["together"], extras="togetherai")
    def get_async_client(self) -> "AsyncTogether":
        from together import AsyncTogether

        return AsyncTogether(api_key=self.api_key.get_secret_value())


@dataclass
class TogetherAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: TogetherAIEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def embed_query(self, query: str) -> list[float]:
        return self._embed_documents(elements=[query])[0]

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        return self._add_embeddings_to_elements(elements, embeddings)

    def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        client = self.config.get_client()
        embeddings = []
        try:
            for batch in batch_generator(
                elements, batch_size=self.config.batch_size or len(elements)
            ):
                outputs = client.embeddings.create(
                    model=self.config.embedder_model_name, input=batch
                )
                embeddings.extend([outputs.data[i].embedding for i in range(len(batch))])
        except Exception as e:
            raise self.wrap_error(e=e)
        return embeddings


@dataclass
class AsyncTogetherAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: TogetherAIEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    async def embed_query(self, query: str) -> list[float]:
        embedding = await self._embed_documents(elements=[query])
        return embedding[0]

    async def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = await self._embed_documents([e.get("text", "") for e in elements])
        return self._add_embeddings_to_elements(elements, embeddings)

    async def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        client = self.config.get_async_client()
        embeddings = []
        try:
            for batch in batch_generator(
                elements, batch_size=self.config.batch_size or len(elements)
            ):
                outputs = await client.embeddings.create(
                    model=self.config.embedder_model_name, input=batch
                )
                embeddings.extend([outputs.data[i].embedding for i in range(len(batch))])
        except Exception as e:
            raise self.wrap_error(e=e)
        return embeddings
