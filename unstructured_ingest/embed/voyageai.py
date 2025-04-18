from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.errors_v2 import ProviderError, UserAuthError, UserError, is_internal_error
from unstructured_ingest.errors_v2 import (
    RateLimitError as CustomRateLimitError,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from voyageai import AsyncClient as AsyncVoyageAIClient
    from voyageai import Client as VoyageAIClient


class VoyageAIEmbeddingConfig(EmbeddingConfig):
    batch_size: int = Field(
        default=32,
        le=128,
        description="Batch size for embedding requests. VoyageAI has a limit of 128.",
    )
    api_key: Optional[SecretStr] = Field(description="API key for VoyageAI", default=None)
    embedder_model_name: str = Field(
        default="voyage-3", alias="model_name", description="VoyageAI model name"
    )
    max_retries: int = Field(default=0, description="Max retries for embedding requests.")
    timeout_in_seconds: Optional[int] = Field(
        default=None, description="Optional timeout in seconds for embedding requests."
    )

    def wrap_error(self, e: Exception) -> Exception:
        if is_internal_error(e=e):
            return e
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

        api_key = self.api_key.get_secret_value() if self.api_key else None
        client = VoyageAIClient(
            api_key=api_key,
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

        api_key = self.api_key.get_secret_value() if self.api_key else None
        client = AsyncVoyageAIClient(
            api_key=api_key,
            max_retries=self.max_retries,
            timeout=self.timeout_in_seconds,
        )
        return client


@dataclass
class VoyageAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: VoyageAIEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> "VoyageAIClient":
        return self.config.get_client()

    def embed_batch(self, client: "VoyageAIClient", batch: list[str]) -> list[list[float]]:
        if self.config.embedder_model_name == "voyage-multimodal-3":
            batch = [[text] for text in batch]
            response = client.multimodal_embed(inputs=batch, model=self.config.embedder_model_name)
        else:
            response = client.embed(texts=batch, model=self.config.embedder_model_name)
        return response.embeddings


@dataclass
class AsyncVoyageAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: VoyageAIEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> "AsyncVoyageAIClient":
        return self.config.get_async_client()

    async def embed_batch(
        self, client: "AsyncVoyageAIClient", batch: list[str]
    ) -> list[list[float]]:
        if self.config.embedder_model_name == "voyage-multimodal-3":
            batch = [[text] for text in batch]
            response = await client.multimodal_embed(
                inputs=batch, model=self.config.embedder_model_name
            )
        else:
            response = await client.embed(texts=batch, model=self.config.embedder_model_name)
        return response.embeddings
