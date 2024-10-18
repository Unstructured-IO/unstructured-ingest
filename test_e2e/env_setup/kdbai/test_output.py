#!/usr/bin/env python3

import click
import kdbai_client as kdbai


@click.command()
@click.option("--endpoint", type=str, default="http://localhost:8082")
def run_tests(endpoint: str):
    session = kdbai.Session(endpoint=endpoint)
    db = session.database("default")
    print("Running document length check")
    documents = db.table("unstructured_test")
    retrieved_len = len(documents.query())
    expected_len = 5
    assert (
        retrieved_len == expected_len
    ), f"length of documents in table {retrieved_len} didn't match expected {expected_len}"
    print("Finished running document length check")


if __name__ == "__main__":
    run_tests()
