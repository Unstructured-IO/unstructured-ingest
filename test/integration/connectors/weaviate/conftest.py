import json
from pathlib import Path

import pytest


@pytest.fixture
def collections_schema_config() -> dict:
    int_test_dir = Path(__file__).parent
    assets_dir = int_test_dir / "assets"
    config_file = assets_dir / "elements.json"
    assert config_file.exists()
    assert config_file.is_file()
    with config_file.open() as config_data:
        return json.load(config_data)
