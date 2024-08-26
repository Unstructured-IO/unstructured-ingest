from pathlib import Path

import pytest
from unstructured.chunking import dispatch
from unstructured.documents.elements import assign_and_map_hash_ids
from unstructured.partition.auto import partition

from unstructured_ingest.utils.chunking import (
    assign_and_map_hash_ids as new_assign_and_map_hash_ids,
)

test_file_path = Path(__file__).resolve()
project_root = test_file_path.parent.parent
docs_path = project_root / "example-docs"


@pytest.mark.parametrize(
    "chunking_strategy",
    ["basic", "by_title"],
)
def test_assign_and_map_hash_ids(chunking_strategy):
    # Make sure the new logic working on dict content matches the
    # results if using the unstructured version
    file_path = docs_path / "book-war-and-peace-1p.txt"
    elements = partition(filename=str(file_path.resolve()), strategy="fast")
    chunked_elements = dispatch.chunk(elements=elements, chunking_strategy=chunking_strategy)
    chunked_elements_copy = chunked_elements.copy()

    hashed_chunked_elements = assign_and_map_hash_ids(chunked_elements)
    og_chunked_elements_dicts = [e.to_dict() for e in hashed_chunked_elements]

    new_chunked_elements_dicts = [e.to_dict() for e in chunked_elements_copy]
    new_chunked_elements_dicts = new_assign_and_map_hash_ids(new_chunked_elements_dicts)

    for e1, e2 in zip(og_chunked_elements_dicts, new_chunked_elements_dicts):
        assert e1 == e2
