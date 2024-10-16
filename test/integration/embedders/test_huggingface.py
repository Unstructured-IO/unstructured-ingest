import json
from pathlib import Path

from test.integration.embedders.utils import validate_embedding_output
from unstructured_ingest.v2.processes.embedder import Embedder, EmbedderConfig


def test_huggingface_embedder(embedder_file: Path):
    embedder_config = EmbedderConfig(embedding_provider="huggingface")
    embedder = Embedder(config=embedder_config)
    results = embedder.run(elements_filepath=embedder_file)
    assert results
    with embedder_file.open("r") as f:
        original_elements = json.load(f)
    validate_embedding_output(original_elements=original_elements, output_elements=results)
