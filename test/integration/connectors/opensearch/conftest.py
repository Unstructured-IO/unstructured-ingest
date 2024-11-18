from pathlib import Path

import pandas as pd
import pytest

FILENAME = "wiki_movie_plots_small.csv"


@pytest.fixture
def movies_dataframe() -> pd.DataFrame:
    int_test_dir = Path(__file__).parent
    assets_dir = int_test_dir / "assets"
    upload_file = assets_dir / FILENAME
    assert upload_file.exists()
    assert upload_file.is_file()
    return pd.read_csv(upload_file).dropna().reset_index()
