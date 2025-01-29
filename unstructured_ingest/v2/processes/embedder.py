import json
from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional

from pydantic import BaseModel, Field, SecretStr

from unstructured_ingest.v2.interfaces.process import BaseProcess

if TYPE_CHECKING:
    from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder


class EmbedderConfig(BaseModel):
    embedding_provider: Optional[
        Literal[
            "openai",
            "azure-openai",
            "huggingface",
            "aws-bedrock",
            "vertexai",
            "voyageai",
            "octoai",
            "mixedbread-ai",
            "togetherai",
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
    embedding_azure_endpoint: Optional[str] = Field(
        default=None,
        description="Your Azure endpoint, including the resource, "
        "e.g. `https://example-resource.azure.openai.com/`",
    )
    embedding_azure_api_version: Optional[str] = Field(
        description="Azure API version", default=None
    )

    def get_huggingface_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured_ingest.embed.huggingface import (
            HuggingFaceEmbeddingConfig,
            HuggingFaceEmbeddingEncoder,
        )

        return HuggingFaceEmbeddingEncoder(
            config=HuggingFaceEmbeddingConfig.model_validate(embedding_kwargs)
        )

    def get_openai_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured_ingest.embed.openai import OpenAIEmbeddingConfig, OpenAIEmbeddingEncoder

        return OpenAIEmbeddingEncoder(config=OpenAIEmbeddingConfig.model_validate(embedding_kwargs))

    def get_azure_openai_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured_ingest.embed.azure_openai import (
            AzureOpenAIEmbeddingConfig,
            AzureOpenAIEmbeddingEncoder,
        )

        config_kwargs = {
            "api_key": self.embedding_api_key,
            "azure_endpoint": self.embedding_azure_endpoint,
        }
        if api_version := self.embedding_azure_api_version:
            config_kwargs["api_version"] = api_version
        if model_name := self.embedding_model_name:
            config_kwargs["model_name"] = model_name

        return AzureOpenAIEmbeddingEncoder(
            config=AzureOpenAIEmbeddingConfig.model_validate(config_kwargs)
        )

    def get_octoai_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured_ingest.embed.octoai import OctoAiEmbeddingConfig, OctoAIEmbeddingEncoder

        return OctoAIEmbeddingEncoder(config=OctoAiEmbeddingConfig.model_validate(embedding_kwargs))

    def get_bedrock_embedder(self) -> "BaseEmbeddingEncoder":
        from unstructured_ingest.embed.bedrock import (
            BedrockEmbeddingConfig,
            BedrockEmbeddingEncoder,
        )

        return BedrockEmbeddingEncoder(
            config=BedrockEmbeddingConfig(
                aws_access_key_id=self.embedding_aws_access_key_id,
                aws_secret_access_key=self.embedding_aws_secret_access_key.get_secret_value(),
                region_name=self.embedding_aws_region,
            )
        )

    def get_vertexai_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured_ingest.embed.vertexai import (
            VertexAIEmbeddingConfig,
            VertexAIEmbeddingEncoder,
        )

        return VertexAIEmbeddingEncoder(
            config=VertexAIEmbeddingConfig.model_validate(embedding_kwargs)
        )

    def get_voyageai_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured_ingest.embed.voyageai import (
            VoyageAIEmbeddingConfig,
            VoyageAIEmbeddingEncoder,
        )

        return VoyageAIEmbeddingEncoder(
            config=VoyageAIEmbeddingConfig.model_validate(embedding_kwargs)
        )

    def get_mixedbread_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured_ingest.embed.mixedbreadai import (
            MixedbreadAIEmbeddingConfig,
            MixedbreadAIEmbeddingEncoder,
        )

        return MixedbreadAIEmbeddingEncoder(
            config=MixedbreadAIEmbeddingConfig.model_validate(embedding_kwargs)
        )

    def get_togetherai_embedder(self, embedding_kwargs: dict) -> "BaseEmbeddingEncoder":
        from unstructured_ingest.embed.togetherai import (
            TogetherAIEmbeddingConfig,
            TogetherAIEmbeddingEncoder,
        )

        return TogetherAIEmbeddingEncoder(
            config=TogetherAIEmbeddingConfig.model_validate(embedding_kwargs)
        )

    def get_embedder(self) -> "BaseEmbeddingEncoder":
        kwargs: dict[str, Any] = {}
        if self.embedding_api_key:
            kwargs["api_key"] = self.embedding_api_key.get_secret_value()
        if self.embedding_model_name:
            kwargs["model_name"] = self.embedding_model_name
        # TODO make this more dynamic to map to encoder configs
        if self.embedding_provider == "openai":
            return self.get_openai_embedder(embedding_kwargs=kwargs)

        if self.embedding_provider == "huggingface":
            return self.get_huggingface_embedder(embedding_kwargs=kwargs)

        if self.embedding_provider == "octoai":
            return self.get_octoai_embedder(embedding_kwargs=kwargs)

        if self.embedding_provider == "aws-bedrock":
            return self.get_bedrock_embedder()

        if self.embedding_provider == "vertexai":
            return self.get_vertexai_embedder(embedding_kwargs=kwargs)

        if self.embedding_provider == "voyageai":
            return self.get_voyageai_embedder(embedding_kwargs=kwargs)
        if self.embedding_provider == "mixedbread-ai":
            return self.get_mixedbread_embedder(embedding_kwargs=kwargs)
        if self.embedding_provider == "togetherai":
            return self.get_togetherai_embedder(embedding_kwargs=kwargs)
        if self.embedding_provider == "azure-openai":
            return self.get_azure_openai_embedder(embedding_kwargs=kwargs)

        raise ValueError(f"{self.embedding_provider} not a recognized encoder")


@dataclass
class Embedder(BaseProcess, ABC):
    config: EmbedderConfig

    def init(self, *kwargs: Any) -> None:
        self.config.get_embedder().initialize()

    def run(self, elements_filepath: Path, **kwargs: Any) -> list[dict]:
        # TODO update base embedder classes to support async
        embedder = self.config.get_embedder()
        with elements_filepath.open("r") as elements_file:
            elements = json.load(elements_file)
        if not elements:
            return [e.to_dict() for e in elements]
        embedded_elements = embedder.embed_documents(elements=elements)
        return embedded_elements
