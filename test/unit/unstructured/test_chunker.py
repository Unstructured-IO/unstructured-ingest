import json
from pathlib import Path

from unstructured.documents.elements import NarrativeText, Title

from unstructured_ingest.processes.chunker import Chunker, ChunkerConfig


def test_local_chunker_respects_combine_text_under_n_chars(tmp_path: Path):
    # Two short sections. by_title combines small sections by default, so with
    # combine_text_under_n_chars=0 they only stay apart if the setting reaches the chunker.
    elements = [
        Title("Section one"),
        NarrativeText("Short text."),
        Title("Section two"),
        NarrativeText("More short text."),
    ]
    elements_filepath = tmp_path / "elements.json"
    elements_filepath.write_text(json.dumps([e.to_dict() for e in elements]))

    chunker = Chunker(
        config=ChunkerConfig(chunking_strategy="by_title", chunk_combine_text_under_n_chars=0)
    )
    chunks = chunker.run(elements_filepath=elements_filepath)

    assert [chunk["text"] for chunk in chunks] == [
        "Section one\n\nShort text.",
        "Section two\n\nMore short text.",
    ]
