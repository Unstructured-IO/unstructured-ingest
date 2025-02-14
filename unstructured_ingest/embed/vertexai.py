# type: ignore
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Optional

from pydantic import Field, Secret, ValidationError
from pydantic.functional_validators import BeforeValidator

from unstructured_ingest.embed.interfaces import (
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.errors import UserAuthError, is_internal_error

if TYPE_CHECKING:
    from vertexai.language_models import TextEmbeddingModel


def conform_string_to_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    raise ValidationError(f"Input could not be mapped to a valid dict: {value}")


ApiKeyType = Secret[Annotated[dict, BeforeValidator(conform_string_to_dict)]]


class VertexAIEmbeddingConfig(EmbeddingConfig):
    api_key: ApiKeyType
    embedder_model_name: Optional[str] = Field(
        default="textembedding-gecko@001", alias="model_name"
    )

    def wrap_error(self, e: Exception) -> Exception:
        if is_internal_error(e=e):
            return e
        from google.auth.exceptions import GoogleAuthError

        if isinstance(e, GoogleAuthError):
            return UserAuthError(e)
        return e

    def register_application_credentials(self):
        # TODO look into passing credentials in directly, rather than via env var and tmp file
        application_credentials_path = Path("/tmp") / "google-vertex-app-credentials.json"
        with application_credentials_path.open("w+") as credentials_file:
            json.dump(self.api_key.get_secret_value(), credentials_file)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(application_credentials_path)

    @requires_dependencies(
        ["vertexai"],
        extras="embed-vertexai",
    )
    def get_client(self) -> "TextEmbeddingModel":
        """Creates a VertexAI python client to embed elements."""
        from vertexai.language_models import TextEmbeddingModel

        self.register_application_credentials()
        return TextEmbeddingModel.from_pretrained(self.embedder_model_name)


@dataclass
class VertexAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: VertexAIEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> "TextEmbeddingModel":
        return self.config.get_client()

    @requires_dependencies(
        ["vertexai"],
        extras="embed-vertexai",
    )
    def embed_batch(self, client: "TextEmbeddingModel", batch: list[str]) -> list[list[float]]:
        from vertexai.language_models import TextEmbeddingInput

        inputs = [TextEmbeddingInput(text=text) for text in batch]
        response = client.get_embeddings(inputs)
        return [e.values for e in response]


@dataclass
class AsyncVertexAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: VertexAIEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> "TextEmbeddingModel":
        return self.config.get_client()

    @requires_dependencies(
        ["vertexai"],
        extras="embed-vertexai",
    )
    async def embed_batch(self, client: Any, batch: list[str]) -> list[list[float]]:
        from vertexai.language_models import TextEmbeddingInput

        inputs = [TextEmbeddingInput(text=text) for text in batch]
        response = await client.get_embeddings_async(inputs)
        return [e.values for e in response]
