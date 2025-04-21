from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.errors_v2 import (
    ProviderError,
    QuotaError,
    RateLimitError,
    UserAuthError,
    UserError,
    is_internal_error,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from openai import AsyncOpenAI, OpenAI


class OctoAiEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr = Field(description="API key for OctoAI")
    embedder_model_name: str = Field(
        default="thenlper/gte-large", alias="model_name", description="octoai model name"
    )
    base_url: str = Field(
        default="https://text.octoai.run/v1", description="optional override for the base url"
    )

    def wrap_error(self, e: Exception) -> Exception:
        if is_internal_error(e=e):
            return e
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

    def run_precheck(self) -> None:
        client = self.get_client()
        try:
            models = [m.id for m in list(client.models.list())]
            if self.embedder_model_name not in models:
                raise UserError(
                    "model '{}' not found: {}".format(self.embedder_model_name, ", ".join(models))
                )
        except Exception as e:
            raise self.wrap_error(e=e)

    @requires_dependencies(
        ["openai", "tiktoken"],
        extras="octoai",
    )
    def get_client(self) -> "OpenAI":
        """Creates an OpenAI python client to embed elements. Uses the OpenAI SDK."""
        from openai import OpenAI

        return OpenAI(api_key=self.api_key.get_secret_value(), base_url=self.base_url)

    @requires_dependencies(
        ["openai", "tiktoken"],
        extras="octoai",
    )
    def get_async_client(self) -> "AsyncOpenAI":
        """Creates an OpenAI python client to embed elements. Uses the OpenAI SDK."""
        from openai import AsyncOpenAI

        return AsyncOpenAI(api_key=self.api_key.get_secret_value(), base_url=self.base_url)


@dataclass
class OctoAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: OctoAiEmbeddingConfig

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def _embed_query(self, query: str):
        client = self.get_client()
        response = client.embeddings.create(input=query, model=self.config.embedder_model_name)
        return response.data[0].embedding

    def get_client(self) -> "OpenAI":
        return self.config.get_client()

    def embed_batch(self, client: "OpenAI", batch: list[str]) -> list[list[float]]:
        response = client.embeddings.create(input=batch, model=self.config.embedder_model_name)
        return [data.embedding for data in response.data]


@dataclass
class AsyncOctoAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: OctoAiEmbeddingConfig

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> "AsyncOpenAI":
        return self.config.get_async_client()

    async def embed_batch(self, client: "AsyncOpenAI", batch: list[str]) -> list[list[float]]:
        response = await client.embeddings.create(
            input=batch, model=self.config.embedder_model_name
        )
        return [data.embedding for data in response.data]
