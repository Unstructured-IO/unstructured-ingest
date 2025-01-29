import json
from pathlib import Path

from test.integration.embedders.utils import validate_embedding_output, validate_raw_embedder
from unstructured_ingest.embed.huggingface import (
    HuggingFaceEmbeddingConfig,
    HuggingFaceEmbeddingEncoder,
)
from unstructured_ingest.v2.processes.embedder import Embedder, EmbedderConfig


def test_huggingface_embedder(embedder_file: Path):
    embedder_config = EmbedderConfig(embedding_provider="huggingface")
    embedder = Embedder(config=embedder_config)
    results = embedder.run(elements_filepath=embedder_file)
    assert results
    with embedder_file.open("r") as f:
        original_elements = json.load(f)
    validate_embedding_output(original_elements=original_elements, output_elements=results)


def test_raw_hugginface_embedder(embedder_file: Path):
    embedder = HuggingFaceEmbeddingEncoder(config=HuggingFaceEmbeddingConfig())
    validate_raw_embedder(embedder=embedder, embedder_file=embedder_file, expected_dimension=384)
