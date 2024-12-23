from pathlib import Path

import pytest

int_test_dir = Path(__file__).parent
assets_dir = int_test_dir / "assets"


@pytest.fixture
def duckdb_schema() -> Path:
    schema_file = assets_dir / "duckdb-schema.sql"
    assert schema_file.exists()
    assert schema_file.is_file()
    return schema_file
