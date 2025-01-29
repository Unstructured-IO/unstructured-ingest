import asyncio
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.utils.data_prep import batch_generator
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
        return self._embed(["Q"])[0]

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

    def _embed(self, texts: list[str]) -> list[list[float]]:
        """
        Embed a list of texts using the Mixedbread AI API.

        Args:
            texts (list[str]): List of texts to embed.

        Returns:
            list[list[float]]: List of embeddings.
        """

        responses = []
        client = self.config.get_client()
        for batch in batch_generator(texts, batch_size=self.config.batch_size or len(texts)):
            response = client.embeddings(
                model=self.config.embedder_model_name,
                normalized=True,
                encoding_format=ENCODING_FORMAT,
                truncation_strategy=TRUNCATION_STRATEGY,
                request_options=self.get_request_options(),
                input=batch,
            )
            responses.append(response)
        return [item.embedding for response in responses for item in response.data]

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        """
        Embed a list of document elements.

        Args:
            elements (list[Element]): List of document elements.

        Returns:
            list[Element]: Elements with embeddings.
        """
        embeddings = self._embed([e.get("text", "") for e in elements])
        return self._add_embeddings_to_elements(elements, embeddings)

    def embed_query(self, query: str) -> list[float]:
        """
        Embed a query string.

        Args:
            query (str): Query string to embed.

        Returns:
            list[float]: Embedding of the query.
        """
        return self._embed([query])[0]


@dataclass
class AsyncMixedbreadAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):

    config: MixedbreadAIEmbeddingConfig

    async def get_exemplary_embedding(self) -> list[float]:
        """Get an exemplary embedding to determine dimensions and unit vector status."""
        embedding = await self._embed(["Q"])
        return embedding[0]

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

    async def _embed(self, texts: list[str]) -> list[list[float]]:
        """
        Embed a list of texts using the Mixedbread AI API.

        Args:
            texts (list[str]): List of texts to embed.

        Returns:
            list[list[float]]: List of embeddings.
        """
        client = self.config.get_async_client()
        tasks = []
        for batch in batch_generator(texts, batch_size=self.config.batch_size or len(texts)):
            tasks.append(
                client.embeddings(
                    model=self.config.embedder_model_name,
                    normalized=True,
                    encoding_format=ENCODING_FORMAT,
                    truncation_strategy=TRUNCATION_STRATEGY,
                    request_options=self.get_request_options(),
                    input=batch,
                )
            )
        responses = await asyncio.gather(*tasks)
        return [item.embedding for response in responses for item in response.data]

    async def embed_documents(self, elements: list[dict]) -> list[dict]:
        """
        Embed a list of document elements.

        Args:
            elements (list[Element]): List of document elements.

        Returns:
            list[Element]: Elements with embeddings.
        """
        embeddings = await self._embed([e.get("text", "") for e in elements])
        return self._add_embeddings_to_elements(elements, embeddings)

    async def embed_query(self, query: str) -> list[float]:
        """
        Embed a query string.

        Args:
            query (str): Query string to embed.

        Returns:
            list[float]: Embedding of the query.
        """
        embedding = await self._embed([query])
        return embedding[0]
