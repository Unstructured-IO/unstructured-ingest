from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

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


class OpenAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr = Field(description="API key for OpenAI")
    embedder_model_name: str = Field(
        default="text-embedding-ada-002", alias="model_name", description="OpenAI model name"
    )
    base_url: Optional[str] = Field(default=None, description="optional override for the base url")

    @requires_dependencies(["openai"], extras="openai")
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

    @requires_dependencies(["openai"], extras="openai")
    def get_models(self) -> Optional[list[str]]:
        # In case the list model endpoint isn't exposed, don't break
        from openai import APIStatusError

        client = self.get_client()
        try:
            models = [m.id for m in list(client.models.list())]
            return models
        except APIStatusError as e:
            if e.status_code == 404:
                return None
        except Exception as e:
            raise self.wrap_error(e=e)

    def run_precheck(self) -> None:
        try:
            models = self.get_models()
            if models is None:
                return
            if self.embedder_model_name not in models:
                raise UserError(
                    "model '{}' not found: {}".format(self.embedder_model_name, ", ".join(models))
                )
        except Exception as e:
            raise self.wrap_error(e=e)

    @requires_dependencies(["openai"], extras="openai")
    def get_client(self) -> "OpenAI":
        from openai import OpenAI

        return OpenAI(api_key=self.api_key.get_secret_value(), base_url=self.base_url)

    @requires_dependencies(["openai"], extras="openai")
    def get_async_client(self) -> "AsyncOpenAI":
        from openai import AsyncOpenAI

        return AsyncOpenAI(api_key=self.api_key.get_secret_value(), base_url=self.base_url)


@dataclass
class OpenAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: OpenAIEmbeddingConfig

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> "OpenAI":
        return self.config.get_client()

    def embed_batch(self, client: "OpenAI", batch: list[str]) -> list[list[float]]:
        response = client.embeddings.create(input=batch, model=self.config.embedder_model_name)
        return [data.embedding for data in response.data]


@dataclass
class AsyncOpenAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: OpenAIEmbeddingConfig

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
