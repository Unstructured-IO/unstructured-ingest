from pathlib import Path

SOURCE_TAG = "source"
DESTINATION_TAG = "destination"

env_setup_path = Path(__file__).parents[4] / "test_e2e" / "env_setup"
expected_results_path = Path(__file__).parents[1] / "expected_results"
