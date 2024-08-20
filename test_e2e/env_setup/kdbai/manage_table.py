#!/usr/bin/env python3


import json

import click
import kdbai_client as kdbai

schema = {
    "columns": [
        {"name": "id", "pytype": "str"},
        {"name": "document", "pytype": "str"},
        {"name": "metadata", "pytype": "dict"},
        {
            "name": "embedding",
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


@click.command()
@click.option("--endpoint", type=str, default="http://localhost:8082")
def manage_table(op, endpoint: str):
    session = kdbai.Session(endpoint=endpoint)
    print(f"Creating kdbai table with schema: {json.dumps(schema)}")
    session.create_table("unstructured_test", schema)
    print("Finished provisioning table")


if __name__ == "__main__":
    manage_table()
