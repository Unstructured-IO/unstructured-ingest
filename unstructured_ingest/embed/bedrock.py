import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, AsyncIterable

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.errors import ProviderError, RateLimitError, UserAuthError, UserError

if TYPE_CHECKING:
    from botocore.client import BaseClient

    class BedrockClient(BaseClient):
        def invoke_model(self, body: str, modelId: str, accept: str, contentType: str) -> dict:
            pass

    class AsyncBedrockClient(BaseClient):
        async def invoke_model(
            self, body: str, modelId: str, accept: str, contentType: str
        ) -> dict:
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
    aws_access_key_id: SecretStr
    aws_secret_access_key: SecretStr
    region_name: str = "us-west-2"
    embed_model_name: str = Field(default="amazon.titan-embed-text-v1", alias="model_name")

    def wrap_error(self, e: Exception) -> Exception:
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

    @requires_dependencies(
        ["boto3", "numpy", "botocore"],
        extras="bedrock",
    )
    def get_client(self) -> "BedrockClient":
        import boto3

        bedrock_client = boto3.client(
            service_name="bedrock-runtime",
            aws_access_key_id=self.aws_access_key_id.get_secret_value(),
            aws_secret_access_key=self.aws_secret_access_key.get_secret_value(),
            region_name=self.region_name,
        )

        return bedrock_client

    @requires_dependencies(
        ["aioboto3"],
        extras="bedrock",
    )
    @asynccontextmanager
    async def get_async_client(self) -> AsyncIterable["AsyncBedrockClient"]:
        import aioboto3

        session = aioboto3.Session()
        async with session.client(
            "bedrock-runtime",
            aws_access_key_id=self.aws_access_key_id.get_secret_value(),
            aws_secret_access_key=self.aws_secret_access_key.get_secret_value(),
            region_name=self.region_name,
        ) as aws_bedrock:
            yield aws_bedrock


@dataclass
class BedrockEmbeddingEncoder(BaseEmbeddingEncoder):
    config: BedrockEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def embed_query(self, query: str) -> list[float]:
        """Call out to Bedrock embedding endpoint."""
        provider = self.config.embed_model_name.split(".")[0]
        body = conform_query(query=query, provider=provider)

        bedrock_client = self.config.get_client()
        # invoke bedrock API
        try:
            response = bedrock_client.invoke_model(
                body=json.dumps(body),
                modelId=self.config.embed_model_name,
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
        embeddings = [self.embed_query(query=e.get("text", "")) for e in elements]
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings


@dataclass
class AsyncBedrockEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: BedrockEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    async def embed_query(self, query: str) -> list[float]:
        """Call out to Bedrock embedding endpoint."""
        provider = self.config.embed_model_name.split(".")[0]
        body = conform_query(query=query, provider=provider)
        try:
            async with self.config.get_async_client() as bedrock_client:
                # invoke bedrock API
                try:
                    response = await bedrock_client.invoke_model(
                        body=json.dumps(body),
                        modelId=self.config.embed_model_name,
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
        embeddings = await asyncio.gather(
            *[self.embed_query(query=e.get("text", "")) for e in elements]
        )
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings
