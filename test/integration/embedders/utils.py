import json
from pathlib import Path
from typing import Optional

from unstructured_ingest.embed.interfaces import BaseEmbeddingEncoder


def validate_embedding_output(original_elements: list[dict], output_elements: list[dict]):
    """
    Make sure the following characteristics are met:
    * The same number of elements are returned
    * For each element that had text, an embeddings entry was added in the output
    * Other than the embedding, nothing about the element was changed
    """
    assert len(original_elements) == len(output_elements)
    for original_element, output_element in zip(original_elements, output_elements):
        if original_element.get("text"):
            assert output_element.get("embeddings", None)
        output_element.pop("embeddings", None)
        assert original_element == output_element


def validate_raw_embedder(
    embedder: BaseEmbeddingEncoder,
    embedder_file: Path,
    expected_dimensions: Optional[tuple[int, ...]] = None,
    expected_is_unit_vector: bool = True,
):
    with open(embedder_file) as f:
        elements = json.load(f)
    all_text = [element["text"] for element in elements]
    single_text = all_text[0]
    num_of_dimensions = embedder.num_of_dimensions
    if expected_dimensions:
        assert (
            num_of_dimensions == expected_dimensions
        ), f"number of dimensions {num_of_dimensions} didn't match expected: {expected_dimensions}"
    is_unit_vector = embedder.is_unit_vector
    assert is_unit_vector == expected_is_unit_vector
    single_embedding = embedder.embed_query(query=single_text)
    expected_length = num_of_dimensions[0]
    assert len(single_embedding) == expected_length
    embedded_elements = embedder.embed_documents(elements=elements)
    validate_embedding_output(original_elements=elements, output_elements=embedded_elements)
