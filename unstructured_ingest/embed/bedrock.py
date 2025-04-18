import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, AsyncIterable

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
    EMBEDDINGS_KEY,
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.errors_v2 import (
    ProviderError,
    RateLimitError,
    UserAuthError,
    UserError,
    is_internal_error,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from botocore.client import BaseClient

    class BedrockRuntimeClient(BaseClient):
        def invoke_model(self, body: str, modelId: str, accept: str, contentType: str) -> dict:
            pass

    class AsyncBedrockRuntimeClient(BaseClient):
        async def invoke_model(
            self, body: str, modelId: str, accept: str, contentType: str
        ) -> dict:
            pass

    class BedrockClient(BaseClient):
        def list_foundation_models(self, byOutputModality: str) -> dict:
            pass


def conform_query(query: str, provider: str) -> dict:
    # replace newlines, which can negatively affect performance.
    text = query.replace(os.linesep, " ")

    # format input body for provider
    input_body = {}
    if provider == "cohere":
        if "input_type" not in input_body:
            input_body["input_type"] = "search_document"
        input_body["texts"] = [text]
    else:
        # includes common provider == "amazon"
        input_body["inputText"] = text
    return input_body


class BedrockEmbeddingConfig(EmbeddingConfig):
    aws_access_key_id: SecretStr = Field(description="aws access key id")
    aws_secret_access_key: SecretStr = Field(description="aws secret access key")
    region_name: str = Field(description="aws region name", default="us-west-2")
    embedder_model_name: str = Field(
        default="amazon.titan-embed-text-v1",
        alias="model_name",
        description="AWS Bedrock model name",
    )

    def wrap_error(self, e: Exception) -> Exception:
        if is_internal_error(e=e):
            return e
        from botocore.exceptions import ClientError

        if isinstance(e, ClientError):
            # https://docs.aws.amazon.com/awssupport/latest/APIReference/CommonErrors.html
            http_response = e.response
            meta = http_response["ResponseMetadata"]
            http_response_code = meta["HTTPStatusCode"]
            error_code = http_response["Error"]["Code"]
            if http_response_code == 400:
                if error_code == "ValidationError":
                    return UserError(http_response["Error"])
                elif error_code == "ThrottlingException":
                    return RateLimitError(http_response["Error"])
                elif error_code == "NotAuthorized" or error_code == "AccessDeniedException":
                    return UserAuthError(http_response["Error"])
            if http_response_code == 403:
                return UserAuthError(http_response["Error"])
            if 400 <= http_response_code < 500:
                return UserError(http_response["Error"])
            if http_response_code >= 500:
                return ProviderError(http_response["Error"])

        logger.error(f"unhandled exception from bedrock: {e}", exc_info=True)
        return e

    def run_precheck(self) -> None:
        client = self.get_bedrock_client()
        try:
            model_info = client.list_foundation_models(byOutputModality="EMBEDDING")
            summaries = model_info.get("modelSummaries", [])
            model_ids = [m["modelId"] for m in summaries]
            arns = [":".join(m["modelArn"]) for m in summaries]

            if self.embedder_model_name not in model_ids and self.embedder_model_name not in arns:
                raise UserError(
                    "model '{}' not found either : {} or {}".format(
                        self.embedder_model_name, ", ".join(model_ids), ", ".join(arns)
                    )
                )
        except Exception as e:
            raise self.wrap_error(e=e)

    def get_client_kwargs(self) -> dict:
        return {
            "aws_access_key_id": self.aws_access_key_id.get_secret_value(),
            "aws_secret_access_key": self.aws_secret_access_key.get_secret_value(),
            "region_name": self.region_name,
        }

    @requires_dependencies(
        ["boto3"],
        extras="bedrock",
    )
    def get_bedrock_client(self) -> "BedrockClient":
        import boto3

        bedrock_client = boto3.client(service_name="bedrock", **self.get_client_kwargs())

        return bedrock_client

    @requires_dependencies(
        ["boto3", "numpy", "botocore"],
        extras="bedrock",
    )
    def get_client(self) -> "BedrockRuntimeClient":
        import boto3

        bedrock_client = boto3.client(service_name="bedrock-runtime", **self.get_client_kwargs())

        return bedrock_client

    @requires_dependencies(
        ["aioboto3"],
        extras="bedrock",
    )
    @asynccontextmanager
    async def get_async_client(self) -> AsyncIterable["AsyncBedrockRuntimeClient"]:
        import aioboto3

        session = aioboto3.Session()
        async with session.client("bedrock-runtime", **self.get_client_kwargs()) as aws_bedrock:
            yield aws_bedrock


@dataclass
class BedrockEmbeddingEncoder(BaseEmbeddingEncoder):
    config: BedrockEmbeddingConfig

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def embed_query(self, query: str) -> list[float]:
        """Call out to Bedrock embedding endpoint."""
        provider = self.config.embedder_model_name.split(".")[0]
        body = conform_query(query=query, provider=provider)

        bedrock_client = self.config.get_client()
        # invoke bedrock API
        try:
            response = bedrock_client.invoke_model(
                body=json.dumps(body),
                modelId=self.config.embedder_model_name,
                accept="application/json",
                contentType="application/json",
            )
        except Exception as e:
            raise self.wrap_error(e=e)

        # format output based on provider
        response_body = json.loads(response.get("body").read())
        if provider == "cohere":
            return response_body.get("embeddings")[0]
        else:
            # includes common provider == "amazon"
            return response_body.get("embedding")

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        elements = elements.copy()
        elements_with_text = [e for e in elements if e.get("text")]
        if not elements_with_text:
            return elements
        embeddings = [self.embed_query(query=e["text"]) for e in elements_with_text]
        for element, embedding in zip(elements_with_text, embeddings):
            element[EMBEDDINGS_KEY] = embedding
        return elements


@dataclass
class AsyncBedrockEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: BedrockEmbeddingConfig

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    async def embed_query(self, query: str) -> list[float]:
        """Call out to Bedrock embedding endpoint."""
        provider = self.config.embedder_model_name.split(".")[0]
        body = conform_query(query=query, provider=provider)
        try:
            async with self.config.get_async_client() as bedrock_client:
                # invoke bedrock API
                try:
                    response = await bedrock_client.invoke_model(
                        body=json.dumps(body),
                        modelId=self.config.embedder_model_name,
                        accept="application/json",
                        contentType="application/json",
                    )
                except Exception as e:
                    raise self.wrap_error(e=e)
                async with response.get("body") as client_response:
                    response_body = await client_response.json()

            # format output based on provider
            if provider == "cohere":
                return response_body.get("embeddings")[0]
            else:
                # includes common provider == "amazon"
                return response_body.get("embedding")
        except Exception as e:
            raise ValueError(f"Error raised by inference endpoint: {e}")

    async def embed_documents(self, elements: list[dict]) -> list[dict]:
        elements = elements.copy()
        elements_with_text = [e for e in elements if e.get("text")]
        embeddings = await asyncio.gather(
            *[self.embed_query(query=e.get("text", "")) for e in elements_with_text]
        )
        for element, embedding in zip(elements_with_text, embeddings):
            element[EMBEDDINGS_KEY] = embedding
        return elements
