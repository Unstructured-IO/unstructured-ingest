from pathlib import Path

import pytest

FILENAME = "DA-1p-with-duplicate-pages.pdf.json"


@pytest.fixture
def upload_file() -> Path:
    int_test_dir = Path(__file__).parent
    assets_dir = int_test_dir / "assets"
    upload_file = assets_dir / FILENAME
    assert upload_file.exists()
    assert upload_file.is_file()
    return upload_file
