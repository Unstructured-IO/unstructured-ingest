import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional

from pydantic import Field, SecretStr, model_validator

from unstructured_ingest.embed.openai import (
    AsyncOpenAIEmbeddingEncoder,
    OpenAIEmbeddingConfig,
    OpenAIEmbeddingEncoder,
)
from unstructured_ingest.error import UserError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.tls import ssl_context_with_optional_ca_override

if TYPE_CHECKING:
    from openai import AsyncAzureOpenAI, AzureOpenAI

AZURE_COGNITIVE_SERVICES_SCOPE = "https://cognitiveservices.azure.com/.default"


class AzureOpenAIEmbeddingConfig(OpenAIEmbeddingConfig):
    api_key: Optional[SecretStr] = Field(
        default=None,
        description="API key for Azure OpenAI. Omit to use workload identity "
        "(tenant_id and client_id).",
    )
    tenant_id: Optional[str] = Field(
        default=None, description="Entra tenant ID for workload identity authentication"
    )
    client_id: Optional[str] = Field(
        default=None,
        description="Entra application (client) ID for workload identity authentication",
    )
    api_version: str = Field(description="Azure API version", default="2024-06-01")
    azure_endpoint: str = Field(description="Azure endpoint")
    embedder_model_name: str = Field(
        default="text-embedding-ada-002", alias="model_name", description="Azure OpenAI model name"
    )

    @model_validator(mode="after")
    def _validate_auth(self) -> "AzureOpenAIEmbeddingConfig":
        has_identity = bool(self.tenant_id) or bool(self.client_id)
        if self.api_key is not None and has_identity:
            raise ValueError("set either api_key or tenant_id and client_id, not both")
        if self.api_key is None and not (self.tenant_id and self.client_id):
            raise ValueError("api_key, or both tenant_id and client_id, is required")
        return self

    @requires_dependencies(["azure.identity"], extras="azure-openai")
    def _get_token_provider(self) -> Callable[[], str]:
        from azure.identity import WorkloadIdentityCredential, get_bearer_token_provider

        token_file = os.environ.get("AZURE_FEDERATED_TOKEN_FILE")
        if not token_file:
            raise UserError(
                "workload identity is not available: AZURE_FEDERATED_TOKEN_FILE is not set"
            )
        credential = WorkloadIdentityCredential(
            tenant_id=self.tenant_id, client_id=self.client_id, token_file_path=token_file
        )
        return get_bearer_token_provider(credential, AZURE_COGNITIVE_SERVICES_SCOPE)

    def _auth_kwargs(self) -> dict:
        if self.api_key is not None:
            return {"api_key": self.api_key.get_secret_value()}
        return {"azure_ad_token_provider": self._get_token_provider()}

    @requires_dependencies(["openai"], extras="openai")
    def run_precheck(self) -> None:
        """
        Check if embedding model can be reached.

        In Azure OpenAI the fetched models list (``client.models.list()``) is the
        base-model catalog available to the resource, NOT the deployments created in
        the Azure AI Foundry instance — and a deployment name is the only valid value
        for the ``model`` parameter. Validating a deployment name against that catalog
        rejects every custom-named deployment, so instead we validate that the given
        deployment can actually be reached by issuing a minimal embeddings request.
        """
        from openai import APIStatusError

        try:
            client = self.get_client()
            client.embeddings.create(input="precheck", model=self.embedder_model_name)
        except APIStatusError as e:
            if e.status_code == 404 and e.code == "DeploymentNotFound":
                raise UserError(f"model '{self.embedder_model_name}' not found: {e.message}") from e
            raise self.wrap_error(e=e)
        except Exception as e:
            raise self.wrap_error(e=e)

    @requires_dependencies(["openai"], extras="openai")
    def get_client(self) -> "AzureOpenAI":
        from openai import AzureOpenAI, DefaultHttpxClient

        client = DefaultHttpxClient(verify=ssl_context_with_optional_ca_override())
        azure_client = AzureOpenAI(
            http_client=client,
            api_version=self.api_version,
            azure_endpoint=self.azure_endpoint,
            **self._auth_kwargs(),
        )
        if self.api_key is None:
            # the SDK prefers a static token from AZURE_OPENAI_AD_TOKEN over the provider
            azure_client._azure_ad_token = None
        return azure_client

    @requires_dependencies(["openai"], extras="openai")
    def get_async_client(self) -> "AsyncAzureOpenAI":
        from openai import AsyncAzureOpenAI, DefaultAsyncHttpxClient

        client = DefaultAsyncHttpxClient(verify=ssl_context_with_optional_ca_override())
        azure_client = AsyncAzureOpenAI(
            http_client=client,
            api_version=self.api_version,
            azure_endpoint=self.azure_endpoint,
            **self._auth_kwargs(),
        )
        if self.api_key is None:
            # the SDK prefers a static token from AZURE_OPENAI_AD_TOKEN over the provider
            azure_client._azure_ad_token = None
        return azure_client


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
