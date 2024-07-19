import json
import os
from pathlib import Path

current_dir = Path(__file__).parent.absolute()

CLUSTER_URL = "http://localhost:9200"
INDEX_NAME = "ingest-test-destination"
USER = os.environ["ELASTIC_USER"]
PASSWORD = os.environ["ELASTIC_PASSWORD"]
MAPPING_PATH = str(current_dir / "elasticsearch_elements_mappings.json")

with open(MAPPING_PATH) as f:
    mappings = json.load(f)
