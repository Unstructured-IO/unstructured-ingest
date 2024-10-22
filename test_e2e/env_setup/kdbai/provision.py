#!/usr/bin/env python3


import json
import time

import click
import kdbai_client as kdbai

schema = [
    {"name": "id", "type": "str"},
    {"name": "element_id", "type": "str"},
    {"name": "document", "type": "str"},
    {"name": "metadata", "type": "general"},
    {"name": "embeddings", "type": "float32s"},
]

INDEX_HNSW = {
    "name": "hnsw",
    "column": "embeddings",
    "type": "hnsw",
    "params": {
        "dims": 384,
        "metric": "L2",
        "efConstruction": 8,
        "M": 8,
    },
}


def get_session(endpoint: str) -> kdbai.Session:
    timeout = 60
    sleep_duration = 5
    start = time.time()
    while time.time() - start < timeout:
        try:
            return kdbai.Session(endpoint)
        except Exception as e:
            print(f"Failed to get client: {e}")
            print(f"sleeping for {sleep_duration} seconds")
            time.sleep(sleep_duration)
    raise TimeoutError(f"Failed to get client after {timeout} seconds")


@click.command()
@click.option("--endpoint", type=str, default="http://localhost:8082")
def create_table(endpoint: str):
    session = get_session(endpoint)
    print("Connecting kdbai database: default")
    db = session.database("default")
    print(f"Creating kdbai table with schema: {json.dumps(schema)}")
    db.create_table("unstructured_test", schema, indexes=[INDEX_HNSW])
    print("Finished provisioning table")


if __name__ == "__main__":
    create_table()
