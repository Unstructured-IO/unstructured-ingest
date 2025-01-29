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
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.errors import UserAuthError

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

    def embed_query(self, query):
        return self._embed_documents(elements=[query])[0]

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings

    @requires_dependencies(
        ["vertexai"],
        extras="embed-vertexai",
    )
    def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        from vertexai.language_models import TextEmbeddingInput

        inputs = [TextEmbeddingInput(text=element) for element in elements]
        client = self.config.get_client()
        embeddings = []
        try:
            for batch in batch_generator(inputs, batch_size=self.config.batch_size or len(inputs)):
                response = client.get_embeddings(batch)
                embeddings.extend([e.values for e in response])
        except Exception as e:
            raise self.wrap_error(e=e)
        return embeddings


@dataclass
class AsyncVertexAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: VertexAIEmbeddingConfig

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    async def embed_query(self, query):
        embedding = await self._embed_documents(elements=[query])
        return embedding[0]

    async def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = await self._embed_documents([e.get("text", "") for e in elements])
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings

    @requires_dependencies(
        ["vertexai"],
        extras="embed-vertexai",
    )
    async def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        from vertexai.language_models import TextEmbeddingInput

        inputs = [TextEmbeddingInput(text=element) for element in elements]
        client = self.config.get_client()
        embeddings = []
        try:
            for batch in batch_generator(inputs, batch_size=self.config.batch_size or len(inputs)):
                response = await client.get_embeddings_async(batch)
                embeddings.extend([e.values for e in response])
        except Exception as e:
            raise self.wrap_error(e=e)
        return embeddings
