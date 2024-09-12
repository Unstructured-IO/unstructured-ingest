from dataclasses import dataclass
from typing import TYPE_CHECKING, List

import numpy as np
from pydantic import SecretStr

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from langchain_community.embeddings import BedrockEmbeddings


class BedrockEmbeddingConfig(EmbeddingConfig):
    aws_access_key_id: SecretStr
    aws_secret_access_key: SecretStr
    region_name: str = "us-west-2"

    @requires_dependencies(
        ["boto3", "numpy", "langchain_community"],
        extras="bedrock",
    )
    def get_client(self) -> "BedrockEmbeddings":
        # delay import only when needed
        import boto3
        from langchain_community.embeddings import BedrockEmbeddings

        bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            aws_access_key_id=self.aws_access_key_id.get_secret_value(),
            aws_secret_access_key=self.aws_secret_access_key.get_secret_value(),
            region_name=self.region_name,
        )

        bedrock_client = BedrockEmbeddings(client=bedrock_runtime)
        return bedrock_client


@dataclass
class BedrockEmbeddingEncoder(BaseEmbeddingEncoder):
    config: BedrockEmbeddingConfig

    def get_exemplary_embedding(self) -> List[float]:
        return self.embed_query(query="Q")

    def num_of_dimensions(self):
        exemplary_embedding = self.get_exemplary_embedding()
        return np.shape(exemplary_embedding)

    def is_unit_vector(self):
        exemplary_embedding = self.get_exemplary_embedding()
        return np.isclose(np.linalg.norm(exemplary_embedding), 1.0)

    def embed_query(self, query):
        bedrock_client = self.config.get_client()
        return np.array(bedrock_client.embed_query(query))

    def embed_documents(self, elements: List[dict]) -> List[dict]:
        bedrock_client = self.config.get_client()
        embeddings = bedrock_client.embed_documents([e.get("text", "") for e in elements])
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings

    def _add_embeddings_to_elements(self, elements, embeddings) -> List[dict]:
        assert len(elements) == len(embeddings)
        elements_w_embedding = []
        for i, element in enumerate(elements):
            element["embeddings"] = embeddings[i]
            elements_w_embedding.append(element)
        return elements
