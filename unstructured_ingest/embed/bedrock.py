import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, AsyncIterable, Literal

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import (
    EMBEDDINGS_KEY,
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.error import (
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
        def invoke_model(
            self,
            body: str,
            modelId: str,
            accept: str,
            contentType: str,
            inferenceProfileId: str = None,
        ) -> dict:
            pass

    class AsyncBedrockRuntimeClient(BaseClient):
        async def invoke_model(
            self,
            body: str,
            modelId: str,
            accept: str,
            contentType: str,
            inferenceProfileId: str = None,
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
    aws_access_key_id: SecretStr | None = Field(description="aws access key id", default=None)
    aws_secret_access_key: SecretStr | None = Field(
        description="aws secret access key", default=None
    )
    region_name: str = Field(
        description="aws region name",
        default_factory=lambda: (
            os.getenv("BEDROCK_REGION_NAME") or os.getenv("AWS_DEFAULT_REGION") or "us-west-2"
        ),
    )
    endpoint_url: str | None = Field(description="custom bedrock endpoint url", default=None)
    access_method: Literal["credentials", "iam", "assume_role"] | None = Field(
        description="authentication method", default=None
    )
    role_arn: str | None = Field(
        description="IAM role ARN for cross-account Bedrock access", default=None
    )
    external_id: SecretStr | None = Field(
        description="External ID for cross-account Bedrock role assumption", default=None
    )
    embedder_model_name: str = Field(
        default="amazon.titan-embed-text-v1",
        alias="model_name",
        description="AWS Bedrock model name",
    )
    inference_profile_id: str | None = Field(
        description="AWS Bedrock inference profile ID",
        default_factory=lambda: os.getenv("BEDROCK_INFERENCE_PROFILE_ID"),
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
        # Validate access method and credentials configuration
        self._effective_access_method()

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
        kwargs = {
            "region_name": self.region_name,
        }

        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url

        method = self._effective_access_method()

        if method == "credentials":
            if self.aws_access_key_id and self.aws_secret_access_key:
                kwargs["aws_access_key_id"] = self.aws_access_key_id.get_secret_value()
                kwargs["aws_secret_access_key"] = self.aws_secret_access_key.get_secret_value()
            else:
                raise ValueError(
                    "Credentials access method requires aws_access_key_id and aws_secret_access_key"
                )
        elif method == "iam":
            # For IAM, boto3 will use default credential chain (IAM roles, environment, etc.)
            pass
        elif method == "assume_role":
            if not (self.role_arn and self.external_id):
                raise ValueError(
                    "AssumeRole access method requires role_arn and external_id"
                )
        else:
            raise ValueError(
                f"Invalid access_method: {self.access_method}. Must be "
                "'credentials', 'iam', or 'assume_role'"
            )

        return kwargs

    def _effective_access_method(self) -> Literal["credentials", "iam", "assume_role"]:
        if self.access_method is not None:
            method = self.access_method
        else:
            has_credentials = bool(self.aws_access_key_id) and bool(self.aws_secret_access_key)
            has_partial_credentials = bool(self.aws_access_key_id) != bool(
                self.aws_secret_access_key
            )
            has_role = bool(self.role_arn) and bool(self.external_id)
            has_partial_role = bool(self.role_arn) != bool(self.external_id)
            if has_partial_credentials:
                raise ValueError(
                    "Credentials auth requires aws_access_key_id and aws_secret_access_key"
                )
            if has_partial_role:
                raise ValueError("AssumeRole auth requires role_arn and external_id")
            if has_credentials and has_role:
                raise ValueError("access_method is required when multiple Bedrock auth shapes exist")
            if has_credentials:
                method = "credentials"
            elif has_role:
                method = "assume_role"
            else:
                method = "iam"

        if method == "credentials" and not (self.aws_access_key_id and self.aws_secret_access_key):
            raise ValueError(
                "Credentials access method requires aws_access_key_id and aws_secret_access_key"
            )
        if method == "assume_role" and not (self.role_arn and self.external_id):
            raise ValueError("AssumeRole access method requires role_arn and external_id")
        return method

    def _external_id_secret(self) -> str:
        if self.external_id is None:
            raise ValueError("AssumeRole access method requires external_id")
        return self.external_id.get_secret_value()

    def _assume_role_client_kwargs(self) -> dict:
        from utic_aws_auth.aws.sts import assume_role, build_session_name

        creds = assume_role(
            role_arn=self.role_arn,
            external_id=self._external_id_secret(),
            session_name=build_session_name(),
        )
        kwargs = {
            "region_name": self.region_name,
            "aws_access_key_id": creds["access_key"],
            "aws_secret_access_key": creds["secret_key"],
            "aws_session_token": creds["token"],
        }
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        return kwargs

    @requires_dependencies(
        ["boto3"],
        extras="bedrock",
    )
    def get_bedrock_client(self) -> "BedrockClient":
        import boto3

        if self._effective_access_method() == "assume_role":
            from utic_aws_auth.aws.sts import create_role_arn_session

            session = create_role_arn_session(
                role_arn=self.role_arn,
                external_id=self._external_id_secret(),
            )
            return session.client(service_name="bedrock", **self.get_client_kwargs())

        bedrock_client = boto3.client(service_name="bedrock", **self.get_client_kwargs())

        return bedrock_client

    @requires_dependencies(
        ["boto3", "numpy", "botocore"],
        extras="bedrock",
    )
    def get_client(self) -> "BedrockRuntimeClient":
        import boto3

        if self._effective_access_method() == "assume_role":
            from utic_aws_auth.aws.sts import create_role_arn_session

            session = create_role_arn_session(
                role_arn=self.role_arn,
                external_id=self._external_id_secret(),
            )
            return session.client(service_name="bedrock-runtime", **self.get_client_kwargs())

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
        client_kwargs = (
            self._assume_role_client_kwargs()
            if self._effective_access_method() == "assume_role"
            else self.get_client_kwargs()
        )
        async with session.client("bedrock-runtime", **client_kwargs) as aws_bedrock:
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
            invoke_params = {
                "body": json.dumps(body),
                "modelId": self.config.embedder_model_name,
                "accept": "application/json",
                "contentType": "application/json",
            }

            # Add inference profile if configured
            if self.config.inference_profile_id:
                invoke_params["inferenceProfileId"] = self.config.inference_profile_id

            response = bedrock_client.invoke_model(**invoke_params)
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
                    invoke_params = {
                        "body": json.dumps(body),
                        "modelId": self.config.embedder_model_name,
                        "accept": "application/json",
                        "contentType": "application/json",
                    }

                    # Add inference profile if configured
                    if self.config.inference_profile_id:
                        invoke_params["inferenceProfileId"] = self.config.inference_profile_id

                    response = await bedrock_client.invoke_model(**invoke_params)
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
