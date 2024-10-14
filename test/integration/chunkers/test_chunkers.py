import asyncio
import json
import os
from pathlib import Path

import pytest

from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.chunker import Chunker, ChunkerConfig

int_test_dir = Path(__file__).parent
assets_dir = int_test_dir / "assets"

chunker_files = [path for path in assets_dir.iterdir() if path.is_file()]


@pytest.mark.parametrize("chunker_file", chunker_files, ids=[path.name for path in chunker_files])
@pytest.mark.parametrize("strategy", ["basic", "by_title"])
@requires_env("UNSTRUCTURED_API_KEY", "UNSTRUCTURED_API_URL")
def test_chunker(chunker_file: Path, strategy: str):
    api_key = os.getenv("UNSTRUCTURED_API_KEY")
    api_url = os.getenv("UNSTRUCTURED_API_URL")

    chunker_config = ChunkerConfig(
        chunking_strategy=strategy,
        chunk_by_api=True,
        chunk_api_key=api_key,
        chunking_endpoint=api_url,
    )
    chunker = Chunker(config=chunker_config)
    results = asyncio.run(chunker.run_async(elements_filepath=chunker_file))
    assert results
    with chunker_file.open("r") as f:
        elements_data = json.load(f)
    assert len(elements_data) >= len(results)
