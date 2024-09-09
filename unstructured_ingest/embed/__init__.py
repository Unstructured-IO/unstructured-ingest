from unstructured_ingest.embed.bedrock import BedrockEmbeddingEncoder
from unstructured_ingest.embed.huggingface import HuggingFaceEmbeddingEncoder
from unstructured_ingest.embed.mixedbreadai import MixedbreadAIEmbeddingEncoder
from unstructured_ingest.embed.octoai import OctoAIEmbeddingEncoder
from unstructured_ingest.embed.openai import OpenAIEmbeddingEncoder
from unstructured_ingest.embed.vertexai import VertexAIEmbeddingEncoder
from unstructured_ingest.embed.voyageai import VoyageAIEmbeddingEncoder

EMBEDDING_PROVIDER_TO_CLASS_MAP = {
    "langchain-openai": OpenAIEmbeddingEncoder,
    "langchain-huggingface": HuggingFaceEmbeddingEncoder,
    "langchain-aws-bedrock": BedrockEmbeddingEncoder,
    "langchain-vertexai": VertexAIEmbeddingEncoder,
    "langchain-voyageai": VoyageAIEmbeddingEncoder,
    "mixedbread-ai": MixedbreadAIEmbeddingEncoder,
    "octoai": OctoAIEmbeddingEncoder,
}
