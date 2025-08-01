from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field

from unstructured_ingest.embed.openai import (
    AsyncOpenAIEmbeddingEncoder,
    OpenAIEmbeddingConfig,
    OpenAIEmbeddingEncoder,
)
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.tls import ssl_context_with_optional_ca_override

if TYPE_CHECKING:
    from openai import AsyncAzureOpenAI, AzureOpenAI


class AzureOpenAIEmbeddingConfig(OpenAIEmbeddingConfig):
    api_version: str = Field(description="Azure API version", default="2024-06-01")
    azure_endpoint: str = Field(description="Azure endpoint")
    embedder_model_name: str = Field(
        default="text-embedding-ada-002", alias="model_name", description="Azure OpenAI model name"
    )

    @requires_dependencies(["openai"], extras="openai")
    def get_client(self) -> "AzureOpenAI":
        from openai import AzureOpenAI, DefaultHttpxClient

        client = DefaultHttpxClient(verify=ssl_context_with_optional_ca_override())
        return AzureOpenAI(
            http_client=client,
            api_key=self.api_key.get_secret_value(),
            api_version=self.api_version,
            azure_endpoint=self.azure_endpoint,
        )

    @requires_dependencies(["openai"], extras="openai")
    def get_async_client(self) -> "AsyncAzureOpenAI":
        from openai import AsyncAzureOpenAI, DefaultAsyncHttpxClient

        client = DefaultAsyncHttpxClient(verify=ssl_context_with_optional_ca_override())
        return AsyncAzureOpenAI(
            http_client=client,
            api_key=self.api_key.get_secret_value(),
            api_version=self.api_version,
            azure_endpoint=self.azure_endpoint,
        )


@dataclass
class AzureOpenAIEmbeddingEncoder(OpenAIEmbeddingEncoder):
    config: AzureOpenAIEmbeddingConfig

    def get_client(self) -> "AzureOpenAI":
        return self.config.get_client()


@dataclass
class AsyncAzureOpenAIEmbeddingEncoder(AsyncOpenAIEmbeddingEncoder):
    config: AzureOpenAIEmbeddingConfig

    def get_client(self) -> "AsyncAzureOpenAI":
        return self.config.get_async_client()
