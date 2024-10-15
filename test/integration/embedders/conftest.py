from pathlib import Path

import pytest


@pytest.fixture
def embedder_file() -> Path:
    int_test_dir = Path(__file__).parent
    assets_dir = int_test_dir / "assets"
    embedder_file = assets_dir / "DA-1p-with-duplicate-pages.pdf.json"
    assert embedder_file.exists()
    assert embedder_file.is_file()
    return embedder_file
