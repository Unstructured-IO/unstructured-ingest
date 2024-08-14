from abc import ABC
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field, SecretStr
from unstructured.documents.elements import Element
from unstructured.embed import EMBEDDING_PROVIDER_TO_CLASS_MAP
from unstructured.embed.interfaces import BaseEmbeddingEncoder
from unstructured.staging.base import elements_from_json

from unstructured_ingest.v2.interfaces.process import BaseProcess

EmbedderProvider = Enum(
    "EmbedderProvider", {v: v for v in EMBEDDING_PROVIDER_TO_CLASS_MAP.keys()}, type=str
)


class EmbedderConfig(BaseModel):
    embedding_provider: Optional[EmbedderProvider] = Field(
        default=None, description="Type of the embedding class to be used."
    )
    embedding_api_key: Optional[SecretStr] = Field(
        default=None,
        description="API key for the embedding model, for the case an API key is needed.",
    )
    embedding_model_name: Optional[str] = Field(
        default=None,
        description="Embedding model name, if needed. "
        "Chooses a particular LLM between different options, to embed with it.",
    )
    embedding_aws_access_key_id: Optional[str] = Field(
        default=None, description="AWS access key used for AWS-based embedders, such as bedrock"
    )
    embedding_aws_secret_access_key: Optional[SecretStr] = Field(
        default=None, description="AWS secret key used for AWS-based embedders, such as bedrock"
    )
    embedding_aws_region: Optional[str] = Field(
        default="us-west-2", description="AWS region used for AWS-based embedders, such as bedrock"
    )

    def get_embedder(self) -> BaseEmbeddingEncoder:
        kwargs: dict[str, Any] = {}
        if self.embedding_api_key:
            kwargs["api_key"] = self.embedding_api_key.get_secret_value()
        if self.embedding_model_name:
            kwargs["model_name"] = self.embedding_model_name
        # TODO make this more dynamic to map to encoder configs
        if self.embedding_provider == "langchain-openai":
            from unstructured.embed.openai import OpenAIEmbeddingConfig, OpenAIEmbeddingEncoder

            return OpenAIEmbeddingEncoder(config=OpenAIEmbeddingConfig(**kwargs))
        elif self.embedding_provider == "langchain-huggingface":
            from unstructured.embed.huggingface import (
                HuggingFaceEmbeddingConfig,
                HuggingFaceEmbeddingEncoder,
            )

            return HuggingFaceEmbeddingEncoder(config=HuggingFaceEmbeddingConfig(**kwargs))
        elif self.embedding_provider == "octoai":
            from unstructured.embed.octoai import OctoAiEmbeddingConfig, OctoAIEmbeddingEncoder

            return OctoAIEmbeddingEncoder(config=OctoAiEmbeddingConfig(**kwargs))
        elif self.embedding_provider == "langchain-aws-bedrock":
            from unstructured.embed.bedrock import BedrockEmbeddingConfig, BedrockEmbeddingEncoder

            return BedrockEmbeddingEncoder(
                config=BedrockEmbeddingConfig(
                    aws_access_key_id=self.embedding_aws_access_key_id,
                    aws_secret_access_key=self.embedding_aws_secret_access_key.get_secret_value(),
                    region_name=self.embedding_aws_region,
                )
            )
        elif self.embedding_provider == "langchain-vertexai":
            from unstructured.embed.vertexai import (
                VertexAIEmbeddingConfig,
                VertexAIEmbeddingEncoder,
            )

            return VertexAIEmbeddingEncoder(config=VertexAIEmbeddingConfig(**kwargs))
        else:
            raise ValueError(f"{self.embedding_provider} not a recognized encoder")


@dataclass
class Embedder(BaseProcess, ABC):
    config: EmbedderConfig

    def run(self, elements_filepath: Path, **kwargs: Any) -> list[Element]:
        # TODO update base embedder classes to support async
        embedder = self.config.get_embedder()
        elements = elements_from_json(filename=str(elements_filepath))
        if not elements:
            return elements
        return embedder.embed_documents(elements=elements)
