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
