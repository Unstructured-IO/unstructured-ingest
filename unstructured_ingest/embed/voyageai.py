from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.logger import logger
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
    from voyageai import Client as VoyageAIClient


class VoyageAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(default="voyage-3", alias="model_name")
    batch_size: Optional[int] = Field(default=None)
    truncation: Optional[bool] = Field(default=None)
    max_retries: int = 0
    timeout_in_seconds: Optional[int] = None

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


@dataclass
class VoyageAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: VoyageAIEmbeddingConfig

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

    def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        client: VoyageAIClient = self.config.get_client()
        try:
            response = client.embed(texts=elements, model=self.config.embedder_model_name)
        except Exception as e:
            self.wrap_error(e=e)
        return response.embeddings

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        return self._add_embeddings_to_elements(elements, embeddings)

    def embed_query(self, query: str) -> list[float]:
        return self._embed_documents(elements=[query])[0]
