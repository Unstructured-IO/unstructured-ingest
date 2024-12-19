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


class OctoAiEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(default="thenlper/gte-large", alias="model_name")
    base_url: str = Field(default="https://text.octoai.run/v1")

    @requires_dependencies(
        ["openai", "tiktoken"],
        extras="embed-octoai",
    )
    def get_client(self) -> "OpenAI":
        """Creates an OpenAI python client to embed elements. Uses the OpenAI SDK."""
        from openai import OpenAI

        return OpenAI(api_key=self.api_key.get_secret_value(), base_url=self.base_url)


@dataclass
class OctoAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: OctoAiEmbeddingConfig

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

    def embed_query(self, query: str):
        try:
            client = self.config.get_client()
            response = client.embeddings.create(input=query, model=self.config.embedder_model_name)
        except Exception as e:
            raise self.wrap_error(e=e)
        return response.data[0].embedding

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        texts = [e.get("text", "") for e in elements]
        try:
            client = self.config.get_client()
            response = client.embeddings.create(input=texts, model=self.config.embedder_model_name)
        except Exception as e:
            raise self.wrap_error(e=e)
        embeddings = [data.embedding for data in response.data]
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings
