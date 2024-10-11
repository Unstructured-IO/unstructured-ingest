#!/usr/bin/env python3

import click
import duckdb


def validate_duckdb(conn: duckdb.DuckDBPyConnection):
    _results = conn.sql("select * from elements").fetchall()
    assert len(_results) > 0, "no results found in duckdb"
    print("validation successful")


@click.command()
@click.option("--database-type", type=str, required=True)
@click.option("--database-path", type=str, required=True)
def run_validation(
    database_type: str,
    database_path: str,
):
    print(f"Validating that {database_type} database {database_path}")
    conn = duckdb.connect(database_path)
    validate_duckdb(conn=conn)
    conn.close()


if __name__ == "__main__":
    run_validation()
