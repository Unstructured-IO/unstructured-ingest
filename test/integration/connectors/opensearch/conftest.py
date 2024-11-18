import json
from pathlib import Path

import pandas as pd
import pytest

int_test_dir = Path(__file__).parent
assets_dir = int_test_dir / "assets"


@pytest.fixture
def movies_dataframe() -> pd.DataFrame:
    movies_file = assets_dir / "wiki_movie_plots_small.csv"
    assert movies_file.exists()
    assert movies_file.is_file()
    return pd.read_csv(movies_file).dropna().reset_index()


@pytest.fixture
def elements_mapping() -> dict:
    elements_mapping_file = assets_dir / "opensearch_elements_mappings.json"
    assert elements_mapping_file.exists()
    assert elements_mapping_file.is_file()
    with elements_mapping_file.open() as fp:
        return json.load(fp)
