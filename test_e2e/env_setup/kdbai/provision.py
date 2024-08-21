#!/usr/bin/env python3


import json
import time

import click
import kdbai_client as kdbai

schema = {
    "columns": [
        {"name": "id", "pytype": "str"},
        {"name": "element_id", "pytype": "str"},
        {"name": "document", "pytype": "str"},
        {"name": "metadata", "pytype": "dict"},
        {
            "name": "embeddings",
            "vectorIndex": {
                "dims": 384,
                "type": "hnsw",
                "metric": "L2",
                "efConstruction": 8,
                "M": 8,
            },
        },
    ]
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
    print(f"Creating kdbai table with schema: {json.dumps(schema)}")
    session.create_table("unstructured_test", schema)
    print("Finished provisioning table")


if __name__ == "__main__":
    create_table()
