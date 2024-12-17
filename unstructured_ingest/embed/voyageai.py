from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from pydantic import Field, SecretStr

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder, EmbeddingConfig
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from voyageai import Client as VoyageAIClient


class VoyageAIEmbeddingConfig(EmbeddingConfig):
    api_key: SecretStr
    embedder_model_name: str = Field(default="voyage-3", alias="model_name")
    batch_size: Optional[int] = Field(default=None)
    truncation: Optional[bool] = Field(default=None)
    max_retries: int = 0
    timeout_in_seconds: Optional[int] = None

    @requires_dependencies(
        ["voyageai"],
        extras="embed-voyageai",
    )
    def get_client(self) -> "VoyageAIClient":
        """Creates a VoyageAI python client to embed elements."""
        from voyageai import Client as VoyageAIClient

        client = VoyageAIClient(
            api_key=self.api_key.get_secret_value(),
            max_retries=self.max_retries,
            timeout=self.timeout_in_seconds,
        )
        return client


@dataclass
class VoyageAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: VoyageAIEmbeddingConfig

    def _embed_documents(self, elements: list[str]) -> list[list[float]]:
        client: VoyageAIClient = self.config.get_client()
        
        if self.config.batch_size is None:
            return client.embed(texts=elements, model=self.config.embedder_model_name).embeddings
        
        embeddings = []
        for i in range(0, len(elements), self.config.batch_size):
            embeddings += client.embed(
                texts=elements[i:i + self.config.batch_size], model=self.config.embedder_model_name
            ).embeddings
        return embeddings

    def embed_documents(self, elements: list[dict]) -> list[dict]:
        embeddings = self._embed_documents([e.get("text", "") for e in elements])
        return self._add_embeddings_to_elements(elements, embeddings)

    def embed_query(self, query: str) -> list[float]:
        return self._embed_documents(elements=[query])[0]
