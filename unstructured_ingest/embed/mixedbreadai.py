import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

USER_AGENT = "@mixedbread-ai/unstructured"
TIMEOUT = 60
MAX_RETRIES = 3
ENCODING_FORMAT = "float"
TRUNCATION_STRATEGY = "end"


if TYPE_CHECKING:
    from mixedbread_ai.client import AsyncMixedbreadAI, MixedbreadAI
    from mixedbread_ai.core import RequestOptions


class MixedbreadAIEmbeddingConfig(EmbeddingConfig):
    """
    Configuration class for Mixedbread AI Embedding Encoder.

    Attributes:
        api_key (str): API key for accessing Mixedbread AI..
        embedder_model_name (str): Name of the model to use for embeddings.
    """

    api_key: SecretStr = Field(
        default_factory=lambda: SecretStr(os.environ.get("MXBAI_API_KEY")),
    )

    embedder_model_name: str = Field(
        default="mixedbread-ai/mxbai-embed-large-v1", alias="model_name"
    )

    @requires_dependencies(
        ["mixedbread_ai"],
        extras="embed-mixedbreadai",
    )
    def get_client(self) -> "MixedbreadAI":
        """
        Create the Mixedbread AI client.

        Returns:
            MixedbreadAI: Initialized client.
        """
        from mixedbread_ai.client import MixedbreadAI

        return MixedbreadAI(
            api_key=self.api_key.get_secret_value(),
        )

    @requires_dependencies(
        ["mixedbread_ai"],
        extras="embed-mixedbreadai",
    )
    def get_async_client(self) -> "AsyncMixedbreadAI":
        from mixedbread_ai.client import AsyncMixedbreadAI

        return AsyncMixedbreadAI(
            api_key=self.api_key.get_secret_value(),
        )


@dataclass
class MixedbreadAIEmbeddingEncoder(BaseEmbeddingEncoder):
    """
    Embedding encoder for Mixedbread AI.

    Attributes:
        config (MixedbreadAIEmbeddingConfig): Configuration for the embedding encoder.
    """

    config: MixedbreadAIEmbeddingConfig

    def get_exemplary_embedding(self) -> list[float]:
        """Get an exemplary embedding to determine dimensions and unit vector status."""
        return self.embed_query(query="Q")

    @requires_dependencies(
        ["mixedbread_ai"],
        extras="embed-mixedbreadai",
    )
    def get_request_options(self) -> "RequestOptions":
        from mixedbread_ai.core import RequestOptions

        return RequestOptions(
            max_retries=MAX_RETRIES,
            timeout_in_seconds=TIMEOUT,
            additional_headers={"User-Agent": USER_AGENT},
        )

    def get_client(self) -> "MixedbreadAI":
        return self.config.get_client()

    def embed_batch(self, client: "MixedbreadAI", batch: list[str]) -> list[list[float]]:
        response = client.embeddings(
            model=self.config.embedder_model_name,
            normalized=True,
            encoding_format=ENCODING_FORMAT,
            truncation_strategy=TRUNCATION_STRATEGY,
            request_options=self.get_request_options(),
            input=batch,
        )
        return [datum.embedding for datum in response.data]


@dataclass
class AsyncMixedbreadAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):

    config: MixedbreadAIEmbeddingConfig

    async def get_exemplary_embedding(self) -> list[float]:
        """Get an exemplary embedding to determine dimensions and unit vector status."""
        return await self.embed_query(query="Q")

    @requires_dependencies(
        ["mixedbread_ai"],
        extras="embed-mixedbreadai",
    )
    def get_request_options(self) -> "RequestOptions":
        from mixedbread_ai.core import RequestOptions

        return RequestOptions(
            max_retries=MAX_RETRIES,
            timeout_in_seconds=TIMEOUT,
            additional_headers={"User-Agent": USER_AGENT},
        )

    def get_client(self) -> "AsyncMixedbreadAI":
        return self.config.get_async_client()

    async def embed_batch(self, client: "AsyncMixedbreadAI", batch: list[str]) -> list[list[float]]:
        response = await client.embeddings(
            model=self.config.embedder_model_name,
            normalized=True,
            encoding_format=ENCODING_FORMAT,
            truncation_strategy=TRUNCATION_STRATEGY,
            request_options=self.get_request_options(),
            input=batch,
        )
        return [datum.embedding for datum in response.data]
