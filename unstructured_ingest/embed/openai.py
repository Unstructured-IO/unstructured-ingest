from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from pydantic import Field, SecretStr, field_serializer, field_validator

from unstructured_ingest.embed.interfaces import (
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.error import (
    ProviderError,
    QuotaError,
    RateLimitError,
    UserAuthError,
    UserError,
    is_internal_error,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.tls import ssl_context_with_optional_ca_override

if TYPE_CHECKING:
    from openai import AsyncOpenAI, OpenAI


class OpenAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr = Field(description="API key for OpenAI")
    embedder_model_name: str = Field(
        default="text-embedding-ada-002", alias="model_name", description="OpenAI model name"
    )
    base_url: Optional[str] = Field(default=None, description="optional override for the base url")

    @requires_dependencies(["openai"], extras="openai")
    def wrap_error(self, e: Exception) -> Exception:
        if is_internal_error(e=e):
            return e
        # https://platform.openai.com/docs/guides/error-codes/api-errors
        from openai import APIStatusError

        if not isinstance(e, APIStatusError):
            logger.error(f"unhandled exception from openai: {e}", exc_info=True)
            raise e
        error_code = e.code
        if 400 <= e.status_code < 500:
            # user error
            if e.status_code == 401:
                return UserAuthError(e.message)
            if e.status_code == 429:
                # 429 indicates rate limit exceeded and quote exceeded
                if error_code == "insufficient_quota":
                    return QuotaError(e.message)
                else:
                    return RateLimitError(e.message)
            return UserError(e.message)
        if e.status_code >= 500:
            return ProviderError(e.message)
        logger.error(f"unhandled exception from openai: {e}", exc_info=True)
        return e

    @requires_dependencies(["openai"], extras="openai")
    def get_models(self) -> Optional[list[str]]:
        # In case the list model endpoint isn't exposed, don't break
        from openai import APIStatusError

        client = self.get_client()
        try:
            models = [m.id for m in list(client.models.list())]
            return models
        except APIStatusError as e:
            if e.status_code == 404:
                return None
        except Exception as e:
            raise self.wrap_error(e=e)

    def run_precheck(self) -> None:
        try:
            models = self.get_models()
            if models is None:
                return
            if self.embedder_model_name not in models:
                raise UserError(
                    "model '{}' not found: {}".format(self.embedder_model_name, ", ".join(models))
                )
        except Exception as e:
            raise self.wrap_error(e=e)

    @requires_dependencies(["openai"], extras="openai")
    def get_client(self) -> "OpenAI":
        from openai import DefaultHttpxClient, OpenAI

        client = DefaultHttpxClient(verify=ssl_context_with_optional_ca_override())
        return OpenAI(
            api_key=self.api_key.get_secret_value(), http_client=client, base_url=self.base_url
        )

    @requires_dependencies(["openai"], extras="openai")
    def get_async_client(self) -> "AsyncOpenAI":
        from openai import AsyncOpenAI, DefaultAsyncHttpxClient

        client = DefaultAsyncHttpxClient(verify=ssl_context_with_optional_ca_override())
        return AsyncOpenAI(
            api_key=self.api_key.get_secret_value(), http_client=client, base_url=self.base_url
        )


class CustomOpenAICompatibleEmbeddingConfig(OpenAIEmbeddingConfig):
    """OpenAI-compatible embedder for self-hosted / gateway endpoints (vLLM, NIM,
    Ollama, LiteLLM, etc.) that authenticate via custom HTTP headers instead of,
    or in addition to, a Bearer token.

    When ``api_key`` is unset, no ``Authorization`` header is emitted to the
    upstream gateway: gateways that authenticate solely via ``default_headers``
    can configure this class with ``default_headers={"X-Custom-Auth": SecretStr(...)}``
    and leave ``api_key`` at its default ``None``.
    """

    api_key: Optional[SecretStr] = Field(
        default=None,
        description=(
            "Upstream provider API key. Optional: when unset, no Authorization "
            "header is emitted, which is appropriate for gateways that "
            "authenticate via default_headers only."
        ),
    )
    default_headers: Optional[dict[str, SecretStr]] = Field(
        default=None,
        description="Extra HTTP headers attached to every request.",
    )

    @field_validator("default_headers")
    @classmethod
    def _no_authorization_override(cls, v):
        if v and any(k.lower() == "authorization" for k in v):
            raise ValueError(
                "'Authorization' header is reserved by the SDK; use 'api_key' instead."
            )
        return v

    @field_serializer("default_headers", when_used="json")
    def _dump_headers(self, v):
        return {k: s.get_secret_value() for k, s in v.items()} if v else None

    @requires_dependencies(["openai"], extras="openai")
    def get_client(self) -> "OpenAI":
        from openai import DefaultHttpxClient, OpenAI

        http_client = DefaultHttpxClient(verify=ssl_context_with_optional_ca_override())
        headers = (
            {k: v.get_secret_value() for k, v in self.default_headers.items()}
            if self.default_headers
            else None
        )
        api_key = (lambda: "") if self.api_key is None else self.api_key.get_secret_value()
        return OpenAI(
            api_key=api_key,
            base_url=self.base_url,
            default_headers=headers,
            http_client=http_client,
        )

    @requires_dependencies(["openai"], extras="openai")
    def get_async_client(self) -> "AsyncOpenAI":
        from openai import AsyncOpenAI, DefaultAsyncHttpxClient

        http_client = DefaultAsyncHttpxClient(verify=ssl_context_with_optional_ca_override())
        headers = (
            {k: v.get_secret_value() for k, v in self.default_headers.items()}
            if self.default_headers
            else None
        )
        async def _empty() -> str:
            return ""

        api_key = _empty if self.api_key is None else self.api_key.get_secret_value()
        return AsyncOpenAI(
            api_key=api_key,
            base_url=self.base_url,
            default_headers=headers,
            http_client=http_client,
        )


@dataclass
class OpenAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: OpenAIEmbeddingConfig

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> "OpenAI":
        return self.config.get_client()

    def embed_batch(self, client: "OpenAI", batch: list[str]) -> list[list[float]]:
        response = client.embeddings.create(input=batch, model=self.config.embedder_model_name)
        return [data.embedding for data in response.data]


@dataclass
class AsyncOpenAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: OpenAIEmbeddingConfig

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> "AsyncOpenAI":
        return self.config.get_async_client()

    async def embed_batch(self, client: "AsyncOpenAI", batch: list[str]) -> list[list[float]]:
        response = await client.embeddings.create(
            input=batch, model=self.config.embedder_model_name
        )
        return [data.embedding for data in response.data]
