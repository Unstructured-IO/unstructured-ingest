from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.errors_v2 import (
    RateLimitError as CustomRateLimitError,
)
from unstructured_ingest.errors_v2 import UserAuthError, UserError, is_internal_error
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from together import AsyncTogether, Together


class TogetherAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr = Field(description="API key for Together AI")
    embedder_model_name: str = Field(
        default="togethercomputer/m2-bert-80M-32k-retrieval",
        alias="model_name",
        description="Together AI model name",
    )

    def wrap_error(self, e: Exception) -> Exception:
        if is_internal_error(e=e):
            return e
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

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> "Together":
        return self.config.get_client()

    def embed_batch(self, client: "Together", batch: list[str]) -> list[list[float]]:
        outputs = client.embeddings.create(model=self.config.embedder_model_name, input=batch)
        return [outputs.data[i].embedding for i in range(len(batch))]


@dataclass
class AsyncTogetherAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: TogetherAIEmbeddingConfig

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> "AsyncTogether":
        return self.config.get_async_client()

    async def embed_batch(self, client: Any, batch: list[str]) -> list[list[float]]:
        outputs = await client.embeddings.create(model=self.config.embedder_model_name, input=batch)
        return [outputs.data[i].embedding for i in range(len(batch))]
