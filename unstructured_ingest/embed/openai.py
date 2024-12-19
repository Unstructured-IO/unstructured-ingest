from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.errors import (
    ProviderError,
    QuotaError,
    RateLimitError,
    UserAuthError,
    UserError,
)

if TYPE_CHECKING:
    from openai import OpenAI


class OpenAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(default="text-embedding-ada-002", alias="model_name")

    @requires_dependencies(["openai"], extras="openai")
    def get_client(self) -> "OpenAI":
        from openai import OpenAI

        return OpenAI(api_key=self.api_key.get_secret_value())


@dataclass
class OpenAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: OpenAIEmbeddingConfig

    def handle_error(self, e: Exception):
        # https://platform.openai.com/docs/guides/error-codes/api-errors
        from openai import APIStatusError

        if not isinstance(e, APIStatusError):
            logger.error(f"unhandled exception from openai: {e}", exc_info=True)
            raise e
        error_code = e.code
        if 400 <= e.status_code < 500:
            # user error
            if e.status_code == 401:
                raise UserAuthError(e.message) from e
            if e.status_code == 429:
                # 429 indicates rate limit exceeded and quote exceeded
                if error_code == "insufficient_quota":
                    raise QuotaError(e.message) from e
                else:
                    raise RateLimitError(e.message) from e
            raise UserError(e.message) from e
        if e.status_code >= 500:
            raise ProviderError(e.message) from e
        logger.error(f"unhandled exception from openai: {e}", exc_info=True)
        raise e

    def embed_query(self, query: str) -> list[float]:

        client = self.config.get_client()
        try:
            response = client.embeddings.create(input=query, model=self.config.embedder_model_name)
        except Exception as e:
            self.handle_error(e=e)
        return response.data[0].embedding

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings
