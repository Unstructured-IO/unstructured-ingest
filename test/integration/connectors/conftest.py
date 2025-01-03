import tempfile
from pathlib import Path
from typing import Generator

import pytest

from unstructured_ingest.v2.logger import logger

FILENAME = Path("DA-1p-with-duplicate-pages.pdf.json")


@pytest.fixture
def upload_file() -> Path:
    int_test_dir = Path(__file__).parent
    assets_dir = int_test_dir / "assets"
    upload_file = assets_dir / FILENAME
    assert upload_file.exists()
    assert upload_file.is_file()
    return upload_file


@pytest.fixture
def upload_file_ndjson() -> Path:
    int_test_dir = Path(__file__).parent
    assets_dir = int_test_dir / "assets"
    upload_file = assets_dir / FILENAME.with_suffix(".ndjson")
    assert upload_file.exists()
    assert upload_file.is_file()
    return upload_file


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        logger.info(f"Created temp dir '{temp_path}'")
        yield temp_path
        logger.info(f"Removing temp dir '{temp_path}'")
