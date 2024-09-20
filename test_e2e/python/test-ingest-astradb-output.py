#!/usr/bin/env python
from typing import Tuple

import click
from astrapy import Collection as AstraDBCollection
from astrapy import DataAPIClient as AstraDBClient
from astrapy import Database as AstraDBDatabase


def get_client(
    token: str,
    api_endpoint: str,
    collection_name: str,
) -> Tuple[AstraDBDatabase, AstraDBCollection]:
    # Create a client object to interact with the Astra DB
    my_client = AstraDBClient()

    # Get the database object
    astra_db = my_client.get_database(
        api_endpoint=api_endpoint,
        token=token,
    )

    # Connect to the newly created collection
    astra_db_collection = astra_db.get_collection(
        name=collection_name,
    )

    return astra_db, astra_db_collection


@click.group(name="astradb-ingest")
@click.option("--token", type=str)
@click.option("--api-endpoint", type=str)
@click.option("--collection-name", type=str, default="collection_test")
@click.option("--embedding-dimension", type=int, default=384)
@click.pass_context
def cli(ctx, token: str, api_endpoint: str, collection_name: str, embedding_dimension: int):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    ctx.ensure_object(dict)

    # Log some info
    print(f"Connecting to Astra DB Collection {collection_name} (dim={embedding_dimension})")

    ctx.obj["db"], ctx.obj["collection"] = get_client(
        token,
        api_endpoint,
        collection_name,
    )


@cli.command()
@click.pass_context
def check(ctx):
    collection_name = ctx.parent.params["collection_name"]
    print(f"Checking contents of Astra DB collection: {collection_name}")

    astra_db_collection = ctx.obj["collection"]

    # Tally up the embeddings
    docs_count = astra_db_collection.count_documents({}, upper_bound=10)

    # Print the results
    expected_embeddings = 3
    print(f"# of embeddings in collection vs expected: {docs_count}/{expected_embeddings}")

    # Check that the assertion is true
    assert docs_count == expected_embeddings, (
        f"Number of rows in generated table ({docs_count})"
        f"doesn't match expected value: {expected_embeddings}"
    )

    # Grab an embedding from the collection and search against itself
    # Should get the same document back as the most similar
    find_one_cursor = astra_db_collection.find({}, projection={"*": True}, limit=1)
    for doc in find_one_cursor:
        find_one = doc

    random_vector = find_one["$vector"]
    random_text = find_one["content"]

    cursor = astra_db_collection.find(
        {},
        projection={"*": True},
        vector=random_vector,
        limit=1,
    )

    find_result = []
    for doc in cursor:
        find_result.append(doc)

    # Check that we retrieved the coded cleats copy data
    assert find_result[0]["content"] == random_text
    print("Vector search complete.")


@cli.command()
@click.pass_context
def up(ctx):
    astra_db = ctx.obj["db"]

    collection_name = ctx.parent.params["collection_name"]
    embedding_dimension = ctx.parent.params["embedding_dimension"]

    print(f"creating collection: {collection_name}")
    astra_db.create_collection(collection_name, dimension=embedding_dimension)

    print(f"successfully created collection: {collection_name}")


@cli.command()
@click.pass_context
def down(ctx):
    astra_db = ctx.obj["db"]
    collection_name = ctx.parent.params["collection_name"]

    print(f"deleting collection: {collection_name}")
    astra_db.drop_collection(collection_name)

    print(f"successfully deleted collection: {collection_name}")


if __name__ == "__main__":
    cli()
