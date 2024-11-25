from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field

from unstructured_ingest.embed.openai import OpenAIEmbeddingConfig, OpenAIEmbeddingEncoder
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from openai import AzureOpenAI


class AzureOpenAIEmbeddingConfig(OpenAIEmbeddingConfig):
    api_version: str = Field(description="Azure API version", default="2024-06-01")
    azure_endpoint: str
    embedder_model_name: str = Field(default="text-embedding-ada-002", alias="model_name")

    @requires_dependencies(["openai"], extras="openai")
    def get_client(self) -> "AzureOpenAI":
        from openai import AzureOpenAI

        return AzureOpenAI(
            api_key=self.api_key.get_secret_value(),
            api_version=self.api_version,
            azure_endpoint=self.azure_endpoint,
        )


@dataclass
class AzureOpenAIEmbeddingEncoder(OpenAIEmbeddingEncoder):
    config: AzureOpenAIEmbeddingConfig
