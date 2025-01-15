from pathlib import Path

SOURCE_TAG = "source"
DESTINATION_TAG = "destination"
BLOB_STORAGE_TAG = "blob_storage"
SQL_TAG = "sql"
NOSQL_TAG = "nosql"
VECTOR_DB_TAG = "vector_db"
GRAPH_DB_TAG = "graph_db"
UNCATEGORIZED_TAG = "uncategorized"

env_setup_path = Path(__file__).parents[1] / "env_setup"
expected_results_path = Path(__file__).parents[1] / "expected_results"
