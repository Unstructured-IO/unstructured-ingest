from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional

from pydantic import BaseModel, Field, SecretStr

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces.process import BaseProcess

if TYPE_CHECKING:
    from unstructured.embed.interfaces import BaseEmbeddingEncoder


class EmbedderConfig(BaseModel):
    embedding_provider: Optional[
        Literal[
            "langchain-openai",
            "langchain-huggingface",
            "langchain-aws-bedrock",
            "langchain-vertexai",
            "langchain-voyageai",
            "octoai",
        ]
    ] = Field(default=None, description="Type of the embedding class to be used.")
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

    @requires_dependencies(dependencies=["unstructured"], extras="embed-huggingface")
    def get_huggingface_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured.embed.huggingface import (
            HuggingFaceEmbeddingConfig,
            HuggingFaceEmbeddingEncoder,
        )

        return HuggingFaceEmbeddingEncoder(config=HuggingFaceEmbeddingConfig(**embedding_kwargs))

    @requires_dependencies(dependencies=["unstructured"], extras="openai")
    def get_openai_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured.embed.openai import OpenAIEmbeddingConfig, OpenAIEmbeddingEncoder

        return OpenAIEmbeddingEncoder(config=OpenAIEmbeddingConfig(**embedding_kwargs))

    @requires_dependencies(dependencies=["unstructured"], extras="embed-octoai")
    def get_octoai_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured.embed.octoai import OctoAiEmbeddingConfig, OctoAIEmbeddingEncoder

        return OctoAIEmbeddingEncoder(config=OctoAiEmbeddingConfig(**embedding_kwargs))

    @requires_dependencies(dependencies=["unstructured"], extras="bedrock")
    def get_bedrock_embedder(self) -> "BaseEmbeddingEncoder":
        from unstructured.embed.bedrock import BedrockEmbeddingConfig, BedrockEmbeddingEncoder

        return BedrockEmbeddingEncoder(
            config=BedrockEmbeddingConfig(
                aws_access_key_id=self.embedding_aws_access_key_id,
                aws_secret_access_key=self.embedding_aws_secret_access_key.get_secret_value(),
                region_name=self.embedding_aws_region,
            )
        )

    @requires_dependencies(dependencies=["unstructured"], extras="embed-vertexai")
    def get_vertexai_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured.embed.vertexai import (
            VertexAIEmbeddingConfig,
            VertexAIEmbeddingEncoder,
        )

        return VertexAIEmbeddingEncoder(config=VertexAIEmbeddingConfig(**embedding_kwargs))

    @requires_dependencies(dependencies=["unstructured"], extras="embed-voyageai")
    def get_voyageai_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured.embed.voyageai import VoyageAIEmbeddingConfig, VoyageAIEmbeddingEncoder

        return VoyageAIEmbeddingEncoder(config=VoyageAIEmbeddingConfig(**embedding_kwargs))

    def get_embedder(self) -> "BaseEmbeddingEncoder":
        kwargs: dict[str, Any] = {}
        if self.embedding_api_key:
            kwargs["api_key"] = self.embedding_api_key.get_secret_value()
        if self.embedding_model_name:
            kwargs["model_name"] = self.embedding_model_name
        # TODO make this more dynamic to map to encoder configs
        if self.embedding_provider == "langchain-openai":
            return self.get_openai_embedder(embedding_kwargs=kwargs)

        if self.embedding_provider == "langchain-huggingface":
            return self.get_huggingface_embedder(embedding_kwargs=kwargs)

        if self.embedding_provider == "octoai":
            return self.get_octoai_embedder(embedding_kwargs=kwargs)

        if self.embedding_provider == "langchain-aws-bedrock":
            return self.get_bedrock_embedder()

        if self.embedding_provider == "langchain-vertexai":
            return self.get_vertexai_embedder(embedding_kwargs=kwargs)

        if self.embedding_provider == "langchain-voyageai":
            return self.get_voyageai_embedder(embedding_kwargs=kwargs)

        raise ValueError(f"{self.embedding_provider} not a recognized encoder")


@dataclass
class Embedder(BaseProcess, ABC):
    config: EmbedderConfig

    @requires_dependencies(dependencies=["unstructured"])
    def run(self, elements_filepath: Path, **kwargs: Any) -> list[dict]:
        from unstructured.staging.base import elements_from_json

        # TODO update base embedder classes to support async
        embedder = self.config.get_embedder()
        elements = elements_from_json(filename=str(elements_filepath))
        if not elements:
            return [e.to_dict() for e in elements]
        embedded_elements = embedder.embed_documents(elements=elements)
        return [e.to_dict() for e in embedded_elements]
