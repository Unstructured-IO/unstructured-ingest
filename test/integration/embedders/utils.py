import json
from pathlib import Path
from typing import Optional

from unstructured_ingest.embed.interfaces import AsyncBaseEmbeddingEncoder, BaseEmbeddingEncoder


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
    expected_dimension: Optional[int] = None,
    expected_is_unit_vector: bool = True,
):
    with open(embedder_file) as f:
        elements = json.load(f)
    all_text = [element["text"] for element in elements]
    single_text = all_text[0]
    dimension = embedder.dimension
    if expected_dimension:
        assert (
            dimension == expected_dimension
        ), f"dimensions {dimension} didn't match expected: {expected_dimension}"
    is_unit_vector = embedder.is_unit_vector
    assert is_unit_vector == expected_is_unit_vector
    single_embedding = embedder.embed_query(query=single_text)
    assert len(single_embedding) == dimension
    embedded_elements = embedder.embed_documents(elements=elements)
    validate_embedding_output(original_elements=elements, output_elements=embedded_elements)


async def validate_raw_embedder_async(
    embedder: AsyncBaseEmbeddingEncoder,
    embedder_file: Path,
    expected_dimension: Optional[int] = None,
    expected_is_unit_vector: bool = True,
):
    with open(embedder_file) as f:
        elements = json.load(f)
    all_text = [element["text"] for element in elements]
    single_text = all_text[0]
    dimension = await embedder.dimension
    if expected_dimension:
        assert (
            dimension == expected_dimension
        ), f"dimension {dimension} didn't match expected: {expected_dimension}"
    is_unit_vector = await embedder.is_unit_vector
    assert is_unit_vector == expected_is_unit_vector
    single_embedding = await embedder.embed_query(query=single_text)
    assert len(single_embedding) == dimension
    embedded_elements = await embedder.embed_documents(elements=elements)
    validate_embedding_output(original_elements=elements, output_elements=embedded_elements)
