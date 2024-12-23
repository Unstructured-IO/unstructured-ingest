from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.errors import (
    RateLimitError as CustomRateLimitError,
)
from unstructured_ingest.v2.errors import (
    UserAuthError,
    UserError,
)

if TYPE_CHECKING:
    from together import Together


class TogetherAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(
        default="togethercomputer/m2-bert-80M-8k-retrieval", alias="model_name"
    )

    @requires_dependencies(["together"], extras="togetherai")
    def get_client(self) -> "Together":
        from together import Together

        return Together(api_key=self.api_key.get_secret_value())


@dataclass
class TogetherAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: TogetherAIEmbeddingConfig

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

    def embed_query(self, query: str) -> list[float]:
        return self._embed_documents(elements=[query])[0]

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        return self._add_embeddings_to_elements(elements, embeddings)

    def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        client = self.config.get_client()
        try:
            outputs = client.embeddings.create(
                model=self.config.embedder_model_name, input=elements
            )
        except Exception as e:
            raise self.wrap_error(e=e)
        return [outputs.data[i].embedding for i in range(len(elements))]
