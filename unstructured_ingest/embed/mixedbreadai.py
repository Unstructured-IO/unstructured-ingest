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
    from mixedbread import AsyncMixedbread, Mixedbread


class MixedbreadAIEmbeddingConfig(EmbeddingConfig):
    """
    Configuration class for Mixedbread AI Embedding Encoder.

    Attributes:
        api_key (str): API key for accessing Mixedbread AI..
        embedder_model_name (str): Name of the model to use for embeddings.
    """

    api_key: SecretStr = Field(
        default_factory=lambda: SecretStr(os.environ.get("MXBAI_API_KEY")),
        description="API key for Mixedbread AI",
    )

    embedder_model_name: str = Field(
        default="mixedbread-ai/mxbai-embed-large-v1",
        alias="model_name",
        description="Mixedbread AI model name",
    )

    @requires_dependencies(
        ["mixedbread"],
        extras="embed-mixedbreadai",
    )
    def get_client(self) -> "Mixedbread":
        """
        Create the Mixedbread AI client.

        Returns:
            Mixedbread: Initialized client.
        """
        from mixedbread import Mixedbread

        return Mixedbread(
            api_key=self.api_key.get_secret_value(),
            max_retries=MAX_RETRIES,
        )

    @requires_dependencies(
        ["mixedbread"],
        extras="embed-mixedbreadai",
    )
    def get_async_client(self) -> "AsyncMixedbread":
        from mixedbread import AsyncMixedbread

        return AsyncMixedbread(
            api_key=self.api_key.get_secret_value(),
            max_retries=MAX_RETRIES,
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
        ["mixedbread"],
        extras="embed-mixedbreadai",
    )
    def get_client(self) -> "Mixedbread":
        return self.config.get_client()

    def embed_batch(self, client: "Mixedbread", batch: list[str]) -> list[list[float]]:
        response = client.embed(
            model=self.config.embedder_model_name,
            input=batch,
            normalized=True,
            encoding_format=ENCODING_FORMAT,
            extra_headers={"User-Agent": USER_AGENT},
            timeout=TIMEOUT,
        )
        return [datum.embedding for datum in response.data]


@dataclass
class AsyncMixedbreadAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: MixedbreadAIEmbeddingConfig

    async def get_exemplary_embedding(self) -> list[float]:
        """Get an exemplary embedding to determine dimensions and unit vector status."""
        return await self.embed_query(query="Q")

    @requires_dependencies(
        ["mixedbread"],
        extras="embed-mixedbreadai",
    )
    def get_client(self) -> "AsyncMixedbread":
        return self.config.get_async_client()

    async def embed_batch(self, client: "AsyncMixedbread", batch: list[str]) -> list[list[float]]:
        response = await client.embed(
            model=self.config.embedder_model_name,
            input=batch,
            normalized=True,
            encoding_format=ENCODING_FORMAT,
            extra_headers={"User-Agent": USER_AGENT},
            timeout=TIMEOUT,
        )
        return [datum.embedding for datum in response.data]
