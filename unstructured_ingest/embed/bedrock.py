import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from botocore.client import BaseClient

    class BedrockClient(BaseClient):
        def invoke_model(self, body: str, modelId: str, trace: str) -> dict:
            pass


class BedrockEmbeddingConfig(EmbeddingConfig):
    aws_access_key_id: SecretStr
    aws_secret_access_key: SecretStr
    region_name: str = "us-west-2"
    embed_model_name: str = Field(default="amazon.titan-embed-text-v1", alias="model_name")

    @requires_dependencies(
        ["boto3", "numpy", "botocore"],
        extras="bedrock",
    )
    def get_client(self) -> "BedrockClient":
        # delay import only when needed
        import boto3

        bedrock_client = boto3.client(
            service_name="bedrock-runtime",
            aws_access_key_id=self.aws_access_key_id.get_secret_value(),
            aws_secret_access_key=self.aws_secret_access_key.get_secret_value(),
            region_name=self.region_name,
        )

        return bedrock_client


@dataclass
class BedrockEmbeddingEncoder(BaseEmbeddingEncoder):
    config: BedrockEmbeddingConfig

    def embed_query(self, query: str) -> list[float]:
        """Call out to Bedrock embedding endpoint."""
        # replace newlines, which can negatively affect performance.
        text = query.replace(os.linesep, " ")

        # format input body for provider
        provider = self.config.embed_model_name.split(".")[0]
        input_body = {}
        if provider == "cohere":
            if "input_type" not in input_body:
                input_body["input_type"] = "search_document"
            input_body["texts"] = [text]
        else:
            # includes common provider == "amazon"
            input_body["inputText"] = text
        body = json.dumps(input_body)

        try:
            bedrock_client = self.config.get_client()
            # invoke bedrock API
            response = bedrock_client.invoke_model(
                body=body,
                modelId=self.config.embed_model_name,
                accept="application/json",
                contentType="application/json",
            )

            # format output based on provider
            response_body = json.loads(response.get("body").read())
            if provider == "cohere":
                return response_body.get("embeddings")[0]
            else:
                # includes common provider == "amazon"
                return response_body.get("embedding")
        except Exception as e:
            raise ValueError(f"Error raised by inference endpoint: {e}")

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = [self.embed_query(query=e.get("text", "")) for e in elements]
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings
