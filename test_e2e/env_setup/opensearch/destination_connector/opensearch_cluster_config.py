import json
from pathlib import Path

current_dir = Path(__file__).parent.absolute()

CLUSTER_URL = "http://localhost:9247"
INDEX_NAME = "ingest-test-destination"
USER = "admin"
PASSWORD = "admin"
MAPPING_PATH = str(current_dir / "opensearch_elements_mappings.json")

with open(MAPPING_PATH) as f:
    mappings = json.load(f)
