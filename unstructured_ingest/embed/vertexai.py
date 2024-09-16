# type: ignore
import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, List, Optional

import numpy as np
from pydantic import Field, Secret, ValidationError
from pydantic.functional_validators import BeforeValidator
from unstructured.utils import FileHandler

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from langchain_google_vertexai import VertexAIEmbeddings


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

    def register_application_credentials(self):
        # TODO look into passing credentials in directly, rather than via env var and tmp file
        application_credentials_path = os.path.join("/tmp", "google-vertex-app-credentials.json")
        credentials_file = FileHandler(application_credentials_path)
        credentials_file.write_file(json.dumps(self.api_key.get_secret_value()))
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = application_credentials_path

    @requires_dependencies(
        ["langchain", "langchain_google_vertexai"],
        extras="embed-vertexai",
    )
    def get_client(self) -> "VertexAIEmbeddings":
        """Creates a Langchain VertexAI python client to embed elements."""
        from langchain_google_vertexai import VertexAIEmbeddings

        self.register_application_credentials()
        vertexai_client = VertexAIEmbeddings(model_name=self.embedder_model_name)
        return vertexai_client


@dataclass
class VertexAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: VertexAIEmbeddingConfig

    def get_exemplary_embedding(self) -> List[float]:
        return self.embed_query(query="A sample query.")

    def num_of_dimensions(self):
        exemplary_embedding = self.get_exemplary_embedding()
        return np.shape(exemplary_embedding)

    def is_unit_vector(self):
        exemplary_embedding = self.get_exemplary_embedding()
        return np.isclose(np.linalg.norm(exemplary_embedding), 1.0)

    def embed_query(self, query):
        client = self.config.get_client()
        result = client.embed_query(str(query))
        return result

    def embed_documents(self, elements: List[dict]) -> List[dict]:
        client = self.config.get_client()
        embeddings = client.embed_documents([e.get("text", "") for e in elements])
        elements_with_embeddings = self._add_embeddings_to_elements(elements, embeddings)
        return elements_with_embeddings

    def _add_embeddings_to_elements(self, elements, embeddings) -> List[dict]:
        assert len(elements) == len(embeddings)
        elements_w_embedding = []
        for i, element in enumerate(elements):
            element["embeddings"] = embeddings[i]
            elements_w_embedding.append(element)
        return elements
